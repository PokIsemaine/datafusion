// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Some of these functions reference the Postgres documentation
// or implementation to ensure compatibility and are subject to
// the Postgres license.

//! String expressions

use std::iter;
use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, GenericStringArray, Int32Array, Int64Array, OffsetSizeTrait,
        StringArray,
    },
    datatypes::DataType,
};
use uuid::Uuid;

use datafusion_common::utils::datafusion_strsim;
use datafusion_common::Result;
use datafusion_common::{
    cast::{as_generic_string_array, as_int64_array, as_string_array},
    exec_err, ScalarValue,
};
use datafusion_expr::ColumnarValue;

/// Returns the character with the given code. chr(0) is disallowed because text data types cannot store that character.
/// chr(65) = 'A'
pub fn chr(args: &[ArrayRef]) -> Result<ArrayRef> {
    let integer_array = as_int64_array(&args[0])?;

    // first map is the iterator, second is for the `Option<_>`
    let result = integer_array
        .iter()
        .map(|integer: Option<i64>| {
            integer
                .map(|integer| {
                    if integer == 0 {
                        exec_err!("null character not permitted.")
                    } else {
                        match core::char::from_u32(integer as u32) {
                            Some(integer) => Ok(integer.to_string()),
                            None => {
                                exec_err!("requested character too large for encoding.")
                            }
                        }
                    }
                })
                .transpose()
        })
        .collect::<Result<StringArray>>()?;

    Ok(Arc::new(result) as ArrayRef)
}

/// Concatenates the text representations of all the arguments. NULL arguments are ignored.
/// concat('abcde', 2, NULL, 22) = 'abcde222'
pub fn concat(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    // do not accept 0 arguments.
    if args.is_empty() {
        return exec_err!(
            "concat was called with {} arguments. It requires at least 1.",
            args.len()
        );
    }

    // first, decide whether to return a scalar or a vector.
    let mut return_array = args.iter().filter_map(|x| match x {
        ColumnarValue::Array(array) => Some(array.len()),
        _ => None,
    });
    if let Some(size) = return_array.next() {
        let result = (0..size)
            .map(|index| {
                let mut owned_string: String = "".to_owned();
                for arg in args {
                    match arg {
                        ColumnarValue::Scalar(ScalarValue::Utf8(maybe_value)) => {
                            if let Some(value) = maybe_value {
                                owned_string.push_str(value);
                            }
                        }
                        ColumnarValue::Array(v) => {
                            if v.is_valid(index) {
                                let v = as_string_array(v).unwrap();
                                owned_string.push_str(v.value(index));
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                Some(owned_string)
            })
            .collect::<StringArray>();

        Ok(ColumnarValue::Array(Arc::new(result)))
    } else {
        // short avenue with only scalars
        let initial = Some("".to_string());
        let result = args.iter().fold(initial, |mut acc, rhs| {
            if let Some(ref mut inner) = acc {
                match rhs {
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) => {
                        inner.push_str(v);
                    }
                    ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {}
                    _ => unreachable!(""),
                };
            };
            acc
        });
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
    }
}

/// Concatenates all but the first argument, with separators. The first argument is used as the separator string, and should not be NULL. Other NULL arguments are ignored.
/// concat_ws(',', 'abcde', 2, NULL, 22) = 'abcde,2,22'
pub fn concat_ws(args: &[ArrayRef]) -> Result<ArrayRef> {
    // downcast all arguments to strings
    let args = args
        .iter()
        .map(|e| as_string_array(e))
        .collect::<Result<Vec<&StringArray>>>()?;

    // do not accept 0 or 1 arguments.
    if args.len() < 2 {
        return exec_err!(
            "concat_ws was called with {} arguments. It requires at least 2.",
            args.len()
        );
    }

    // first map is the iterator, second is for the `Option<_>`
    let result = args[0]
        .iter()
        .enumerate()
        .map(|(index, x)| {
            x.map(|sep: &str| {
                let string_vec = args[1..]
                    .iter()
                    .flat_map(|arg| {
                        if !arg.is_null(index) {
                            Some(arg.value(index))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<&str>>();
                string_vec.join(sep)
            })
        })
        .collect::<StringArray>();

    Ok(Arc::new(result) as ArrayRef)
}

/// Converts the first letter of each word to upper case and the rest to lower case. Words are sequences of alphanumeric characters separated by non-alphanumeric characters.
/// initcap('hi THOMAS') = 'Hi Thomas'
pub fn initcap<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = as_generic_string_array::<T>(&args[0])?;

    // first map is the iterator, second is for the `Option<_>`
    let result = string_array
        .iter()
        .map(|string| {
            string.map(|string: &str| {
                let mut char_vector = Vec::<char>::new();
                let mut previous_character_letter_or_number = false;
                for c in string.chars() {
                    if previous_character_letter_or_number {
                        char_vector.push(c.to_ascii_lowercase());
                    } else {
                        char_vector.push(c.to_ascii_uppercase());
                    }
                    previous_character_letter_or_number = c.is_ascii_uppercase()
                        || c.is_ascii_lowercase()
                        || c.is_ascii_digit();
                }
                char_vector.iter().collect::<String>()
            })
        })
        .collect::<GenericStringArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

/// Returns the position of the first occurrence of substring in string.
/// The position is counted from 1. If the substring is not found, returns 0.
/// For example, instr('Helloworld', 'world') = 6.
pub fn instr<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = as_generic_string_array::<T>(&args[0])?;
    let substr_array = as_generic_string_array::<T>(&args[1])?;

    match args[0].data_type() {
        DataType::Utf8 => {
            let result = string_array
                .iter()
                .zip(substr_array.iter())
                .map(|(string, substr)| match (string, substr) {
                    (Some(string), Some(substr)) => string
                        .find(substr)
                        .map_or(Some(0), |index| Some((index + 1) as i32)),
                    _ => None,
                })
                .collect::<Int32Array>();

            Ok(Arc::new(result) as ArrayRef)
        }
        DataType::LargeUtf8 => {
            let result = string_array
                .iter()
                .zip(substr_array.iter())
                .map(|(string, substr)| match (string, substr) {
                    (Some(string), Some(substr)) => string
                        .find(substr)
                        .map_or(Some(0), |index| Some((index + 1) as i64)),
                    _ => None,
                })
                .collect::<Int64Array>();

            Ok(Arc::new(result) as ArrayRef)
        }
        other => {
            exec_err!(
                "instr was called with {other} datatype arguments. It requires Utf8 or LargeUtf8."
            )
        }
    }
}

/// Repeats string the specified number of times.
/// repeat('Pg', 4) = 'PgPgPgPg'
pub fn repeat<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = as_generic_string_array::<T>(&args[0])?;
    let number_array = as_int64_array(&args[1])?;

    let result = string_array
        .iter()
        .zip(number_array.iter())
        .map(|(string, number)| match (string, number) {
            (Some(string), Some(number)) => Some(string.repeat(number as usize)),
            _ => None,
        })
        .collect::<GenericStringArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

/// Replaces all occurrences in string of substring from with substring to.
/// replace('abcdefabcdef', 'cd', 'XX') = 'abXXefabXXef'
pub fn replace<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = as_generic_string_array::<T>(&args[0])?;
    let from_array = as_generic_string_array::<T>(&args[1])?;
    let to_array = as_generic_string_array::<T>(&args[2])?;

    let result = string_array
        .iter()
        .zip(from_array.iter())
        .zip(to_array.iter())
        .map(|((string, from), to)| match (string, from, to) {
            (Some(string), Some(from), Some(to)) => Some(string.replace(from, to)),
            _ => None,
        })
        .collect::<GenericStringArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

/// Splits string at occurrences of delimiter and returns the n'th field (counting from one).
/// split_part('abc~@~def~@~ghi', '~@~', 2) = 'def'
pub fn split_part<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = as_generic_string_array::<T>(&args[0])?;
    let delimiter_array = as_generic_string_array::<T>(&args[1])?;
    let n_array = as_int64_array(&args[2])?;
    let result = string_array
        .iter()
        .zip(delimiter_array.iter())
        .zip(n_array.iter())
        .map(|((string, delimiter), n)| match (string, delimiter, n) {
            (Some(string), Some(delimiter), Some(n)) => {
                if n <= 0 {
                    exec_err!("field position must be greater than zero")
                } else {
                    let split_string: Vec<&str> = string.split(delimiter).collect();
                    match split_string.get(n as usize - 1) {
                        Some(s) => Ok(Some(*s)),
                        None => Ok(Some("")),
                    }
                }
            }
            _ => Ok(None),
        })
        .collect::<Result<GenericStringArray<T>>>()?;

    Ok(Arc::new(result) as ArrayRef)
}

/// Returns true if string starts with prefix.
/// starts_with('alphabet', 'alph') = 't'
pub fn starts_with<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let left = as_generic_string_array::<T>(&args[0])?;
    let right = as_generic_string_array::<T>(&args[1])?;

    let result = arrow::compute::kernels::comparison::starts_with(left, right)?;

    Ok(Arc::new(result) as ArrayRef)
}

/// Returns true if string ends with suffix.
/// ends_with('alphabet', 'abet') = 't'
pub fn ends_with<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let left = as_generic_string_array::<T>(&args[0])?;
    let right = as_generic_string_array::<T>(&args[1])?;

    let result = arrow::compute::kernels::comparison::ends_with(left, right)?;

    Ok(Arc::new(result) as ArrayRef)
}

/// Prints random (v4) uuid values per row
/// uuid() = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'
pub fn uuid(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let len: usize = match &args[0] {
        ColumnarValue::Array(array) => array.len(),
        _ => return exec_err!("Expect uuid function to take no param"),
    };

    let values = iter::repeat_with(|| Uuid::new_v4().to_string()).take(len);
    let array = GenericStringArray::<i32>::from_iter_values(values);
    Ok(ColumnarValue::Array(Arc::new(array)))
}

/// OVERLAY(string1 PLACING string2 FROM integer FOR integer2)
/// Replaces a substring of string1 with string2 starting at the integer bit
/// pgsql overlay('Txxxxas' placing 'hom' from 2 for 4) → Thomas
/// overlay('Txxxxas' placing 'hom' from 2) -> Thomxas, without for option, str2's len is instead
pub fn overlay<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args.len() {
        3 => {
            let string_array = as_generic_string_array::<T>(&args[0])?;
            let characters_array = as_generic_string_array::<T>(&args[1])?;
            let pos_num = as_int64_array(&args[2])?;

            let result = string_array
                .iter()
                .zip(characters_array.iter())
                .zip(pos_num.iter())
                .map(|((string, characters), start_pos)| {
                    match (string, characters, start_pos) {
                        (Some(string), Some(characters), Some(start_pos)) => {
                            let string_len = string.chars().count();
                            let characters_len = characters.chars().count();
                            let replace_len = characters_len as i64;
                            let mut res =
                                String::with_capacity(string_len.max(characters_len));

                            //as sql replace index start from 1 while string index start from 0
                            if start_pos > 1 && start_pos - 1 < string_len as i64 {
                                let start = (start_pos - 1) as usize;
                                res.push_str(&string[..start]);
                            }
                            res.push_str(characters);
                            // if start + replace_len - 1 >= string_length, just to string end
                            if start_pos + replace_len - 1 < string_len as i64 {
                                let end = (start_pos + replace_len - 1) as usize;
                                res.push_str(&string[end..]);
                            }
                            Ok(Some(res))
                        }
                        _ => Ok(None),
                    }
                })
                .collect::<Result<GenericStringArray<T>>>()?;
            Ok(Arc::new(result) as ArrayRef)
        }
        4 => {
            let string_array = as_generic_string_array::<T>(&args[0])?;
            let characters_array = as_generic_string_array::<T>(&args[1])?;
            let pos_num = as_int64_array(&args[2])?;
            let len_num = as_int64_array(&args[3])?;

            let result = string_array
                .iter()
                .zip(characters_array.iter())
                .zip(pos_num.iter())
                .zip(len_num.iter())
                .map(|(((string, characters), start_pos), len)| {
                    match (string, characters, start_pos, len) {
                        (Some(string), Some(characters), Some(start_pos), Some(len)) => {
                            let string_len = string.chars().count();
                            let characters_len = characters.chars().count();
                            let replace_len = len.min(string_len as i64);
                            let mut res =
                                String::with_capacity(string_len.max(characters_len));

                            //as sql replace index start from 1 while string index start from 0
                            if start_pos > 1 && start_pos - 1 < string_len as i64 {
                                let start = (start_pos - 1) as usize;
                                res.push_str(&string[..start]);
                            }
                            res.push_str(characters);
                            // if start + replace_len - 1 >= string_length, just to string end
                            if start_pos + replace_len - 1 < string_len as i64 {
                                let end = (start_pos + replace_len - 1) as usize;
                                res.push_str(&string[end..]);
                            }
                            Ok(Some(res))
                        }
                        _ => Ok(None),
                    }
                })
                .collect::<Result<GenericStringArray<T>>>()?;
            Ok(Arc::new(result) as ArrayRef)
        }
        other => {
            exec_err!("overlay was called with {other} arguments. It requires 3 or 4.")
        }
    }
}

///Returns the Levenshtein distance between the two given strings.
/// LEVENSHTEIN('kitten', 'sitting') = 3
pub fn levenshtein<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!(
            "levenshtein function requires two arguments, got {}",
            args.len()
        );
    }
    let str1_array = as_generic_string_array::<T>(&args[0])?;
    let str2_array = as_generic_string_array::<T>(&args[1])?;
    match args[0].data_type() {
        DataType::Utf8 => {
            let result = str1_array
                .iter()
                .zip(str2_array.iter())
                .map(|(string1, string2)| match (string1, string2) {
                    (Some(string1), Some(string2)) => {
                        Some(datafusion_strsim::levenshtein(string1, string2) as i32)
                    }
                    _ => None,
                })
                .collect::<Int32Array>();
            Ok(Arc::new(result) as ArrayRef)
        }
        DataType::LargeUtf8 => {
            let result = str1_array
                .iter()
                .zip(str2_array.iter())
                .map(|(string1, string2)| match (string1, string2) {
                    (Some(string1), Some(string2)) => {
                        Some(datafusion_strsim::levenshtein(string1, string2) as i64)
                    }
                    _ => None,
                })
                .collect::<Int64Array>();
            Ok(Arc::new(result) as ArrayRef)
        }
        other => {
            exec_err!(
                "levenshtein was called with {other} datatype arguments. It requires Utf8 or LargeUtf8."
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Int32Array;
    use arrow_array::Int64Array;

    use datafusion_common::cast::as_int32_array;

    use super::*;

    #[test]
    fn to_overlay() -> Result<()> {
        let string =
            Arc::new(StringArray::from(vec!["123", "abcdefg", "xyz", "Txxxxas"]));
        let replace_string =
            Arc::new(StringArray::from(vec!["abc", "qwertyasdfg", "ijk", "hom"]));
        let start = Arc::new(Int64Array::from(vec![4, 1, 1, 2])); // start
        let end = Arc::new(Int64Array::from(vec![5, 7, 2, 4])); // replace len

        let res = overlay::<i32>(&[string, replace_string, start, end]).unwrap();
        let result = as_generic_string_array::<i32>(&res).unwrap();
        let expected = StringArray::from(vec!["abc", "qwertyasdfg", "ijkz", "Thomas"]);
        assert_eq!(&expected, result);

        Ok(())
    }

    #[test]
    fn to_levenshtein() -> Result<()> {
        let string1_array =
            Arc::new(StringArray::from(vec!["123", "abc", "xyz", "kitten"]));
        let string2_array =
            Arc::new(StringArray::from(vec!["321", "def", "zyx", "sitting"]));
        let res = levenshtein::<i32>(&[string1_array, string2_array]).unwrap();
        let result =
            as_int32_array(&res).expect("failed to initialized function levenshtein");
        let expected = Int32Array::from(vec![2, 3, 2, 3]);
        assert_eq!(&expected, result);

        Ok(())
    }
}

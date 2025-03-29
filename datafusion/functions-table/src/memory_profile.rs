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

use arrow::array::{StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion_catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion_common::Result;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_expr::{Expr, TableType};
use datafusion_physical_plan::ExecutionPlan;
use std::sync::Arc;

#[derive(Debug, Clone)]
struct TemporaryFilesTable {
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for TemporaryFilesTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let runtime_env = state.runtime_env();
        let temporary_files = runtime_env.disk_manager.get_temporary_files();
        let paths = temporary_files
            .iter()
            .map(|file| file.0.clone())
            .collect::<Vec<_>>();
        let sizes = temporary_files
            .iter()
            .map(|file| file.1)
            .collect::<Vec<_>>();
        let batches = vec![RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(StringArray::from(paths)),
                Arc::new(UInt64Array::from(sizes)),
            ],
        )?];

        Ok(MemorySourceConfig::try_new_exec(
            &[batches],
            TableProvider::schema(self),
            projection.cloned(),
        )?)
    }
}

/// A table function that returns temporary files with their paths and sizes
#[derive(Debug)]
pub struct TemporaryFilesFunc {}

impl TableFunctionImpl for TemporaryFilesFunc {
    fn call(&self, _exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        // Create the schema for the table
        let schema = Arc::new(Schema::new(vec![
            Field::new("path", DataType::Utf8, false),
            Field::new("size", DataType::UInt64, false),
        ]));

        // Create a MemTable plan that returns the RecordBatch
        let provider = TemporaryFilesTable {
            schema,
        };

        Ok(Arc::new(provider))
    }
}

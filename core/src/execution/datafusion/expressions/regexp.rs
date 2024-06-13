/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::{errors::CometError, execution::datafusion::expressions::utils::down_cast_any_ref};
use arrow_array::{builder::BooleanBuilder, Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Schema};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::ScalarValue;
use datafusion_physical_expr::PhysicalExpr;
use regex::Regex;
use std::{
    any::Any,
    fmt::{Display, Formatter},
    hash::Hasher,
    sync::Arc,
};

#[derive(Debug, Hash)]
pub struct RLike {
    child: Arc<dyn PhysicalExpr>,
    pattern: Arc<dyn PhysicalExpr>,
}

impl RLike {
    pub fn new(child: Arc<dyn PhysicalExpr>, pattern: Arc<dyn PhysicalExpr>) -> Self {
        Self { child, pattern }
    }
}

impl Display for RLike {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RLike [child: {}, pattern: {}] ",
            self.child, self.pattern
        )
    }
}

impl PartialEq<dyn Any> for RLike {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self.child.eq(&x.child) && self.pattern.eq(&x.pattern))
            .unwrap_or(false)
    }
}

impl PhysicalExpr for RLike {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> datafusion_common::Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, input_schema: &Schema) -> datafusion_common::Result<bool> {
        self.child.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion_common::Result<ColumnarValue> {
        if let ColumnarValue::Array(v) = self.child.evaluate(batch)? {
            if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(pattern))) =
                self.pattern.evaluate(batch)?
            {
                // TODO cache Regex across invocations of evaluate() or create it in constructor
                match Regex::new(&pattern) {
                    Ok(re) => {
                        let inputs = v
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .expect("string array");
                        let mut builder = BooleanBuilder::with_capacity(inputs.len());
                        if inputs.is_nullable() {
                            for i in 0..inputs.len() {
                                if inputs.is_null(i) {
                                    builder.append_null();
                                } else {
                                    builder.append_value(re.is_match(inputs.value(i)));
                                }
                            }
                        } else {
                            for i in 0..inputs.len() {
                                builder.append_value(re.is_match(inputs.value(i)));
                            }
                        }
                        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
                    }
                    Err(e) => Err(CometError::Internal(format!(
                        "Failed to compile regular expression: {e:?}"
                    ))
                    .into()),
                }
            } else {
                Err(
                    CometError::Internal("Only scalar regex patterns are supported".to_string())
                        .into(),
                )
            }
        } else {
            // this should be unreachable because Spark will evaluate regex expressions against
            // literal strings as part of query planning
            Err(CometError::Internal("Only columnar inputs are supported".to_string()).into())
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        assert!(children.len() == 2);
        Ok(Arc::new(RLike::new(
            children[0].clone(),
            children[1].clone(),
        )))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        use std::hash::Hash;
        let mut s = state;
        self.hash(&mut s);
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use arrow_array::builder::{StringBuilder, StringDictionaryBuilder};
    use arrow_array::{Array, BooleanArray, RecordBatch};
    use arrow_array::types::Int32Type;
    use arrow_schema::{ArrowError, DataType, Field, Schema};
    use datafusion_common::{DataFusionError, ScalarValue};
    use datafusion_expr::ColumnarValue;
    use datafusion_physical_expr::expressions::Literal;
    use datafusion_physical_expr_common::expressions::column::Column;
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
    use super::*;

    #[test]
    fn test_string_input() -> Result<(), DataFusionError> {
        do_test(0, "5[0-9]5", 10)
    }

    #[test]
    fn test_dict_encoded_string_input() -> Result<(), DataFusionError> {
        do_test(1, "5[0-9]5", 10)
    }

    fn do_test(column: usize, pattern: &str, expected_count: usize) -> Result<(), DataFusionError> {
        let batch = create_utf8_batch()?;
        let child_expr = Arc::new(Column::new("foo", column));
        let pattern_expr = Arc::new(Literal::new(ScalarValue::Utf8(Some(pattern.to_string()))));
        let rlike  = RLike::new(child_expr, pattern_expr);
        if let ColumnarValue::Array(array) = rlike.evaluate(&batch).unwrap() {
            let array = array.as_any().downcast_ref::<BooleanArray>().expect("boolean array");
            assert_eq!(expected_count, array.true_count());
        } else {
            unreachable!()
        }
        Ok(())
    }

    fn create_utf8_batch() -> Result<RecordBatch, ArrowError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)), true)
        ]));
        let mut string_builder = StringBuilder::new();
        let mut string_dict_builder = StringDictionaryBuilder::<Int32Type>::new();
        for i in 0..1000 {
            if i % 10 == 0 {
                string_builder.append_null();
                string_dict_builder.append_null();
            } else {
                string_builder.append_value(format!("{}", i));
                string_dict_builder.append_value(format!("{}", i));
            }
        }
        let string_array = string_builder.finish();
        let string_dict_array2 = string_dict_builder.finish();
        RecordBatch::try_new(schema.clone(), vec![Arc::new(string_array), Arc::new(string_dict_array2)])
    }

}
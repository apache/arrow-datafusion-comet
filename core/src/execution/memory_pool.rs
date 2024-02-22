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

use std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    sync::{
        atomic::{AtomicUsize, Ordering::Relaxed},
        Arc,
    },
};

use jni::objects::GlobalRef;

use datafusion::{
    common::DataFusionError,
    execution::memory_pool::{MemoryPool, MemoryReservation},
};

use crate::jvm_bridge::{jni_call, JVMClasses};

pub struct CometMemoryPool {
    task_memory_manager_handle: Arc<GlobalRef>,
    used: AtomicUsize,
}

impl Debug for CometMemoryPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("CometMemoryPool")
            .field("used", &self.used.load(Relaxed))
            .finish()
    }
}

impl CometMemoryPool {
    pub fn new(task_memory_manager_handle: Arc<GlobalRef>) -> CometMemoryPool {
        Self {
            task_memory_manager_handle,
            used: AtomicUsize::new(0),
        }
    }
}

unsafe impl Send for CometMemoryPool {}
unsafe impl Sync for CometMemoryPool {}

impl MemoryPool for CometMemoryPool {
    fn grow(&self, _: &MemoryReservation, additional: usize) {
        self.used.fetch_add(additional, Relaxed);
    }

    fn shrink(&self, _: &MemoryReservation, size: usize) {
        let mut env = JVMClasses::get_env();
        let handle = self.task_memory_manager_handle.as_obj();
        unsafe {
            jni_call!(&mut env, comet_task_memory_manager(handle).release_memory(size as i64) -> ())
                .unwrap();
        }
        self.used.fetch_sub(size, Relaxed);
    }

    fn try_grow(&self, _: &MemoryReservation, additional: usize) -> Result<(), DataFusionError> {
        if additional > 0 {
            let mut env = JVMClasses::get_env();
            let handle = self.task_memory_manager_handle.as_obj();
            unsafe {
                let acquired = jni_call!(&mut env,
                  comet_task_memory_manager(handle).acquire_memory(additional as i64) -> i64)?;

                // If the number of bytes we acquired is less than the requested, return an error,
                // and hopefully will trigger spilling from the caller side.
                if acquired < additional as i64 {
                    return Err(DataFusionError::Execution(format!(
                        "Failed to acquire {} bytes, only got {}. Reserved: {}",
                        additional,
                        acquired,
                        self.reserved(),
                    )));
                }
            }
            self.used.fetch_add(additional, Relaxed);
        }
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.used.load(Relaxed)
    }
}

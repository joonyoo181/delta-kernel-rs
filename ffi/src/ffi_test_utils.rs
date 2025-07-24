//! Utility functions used for tests in this crate.

use crate::error::{EngineError, ExternResult, KernelError};
use crate::{KernelStringSlice, NullableCvoid, TryFromStringSlice};
use std::os::raw::c_void;
use std::ptr::NonNull;

// Used to allocate EngineErrors with test information from Rust tests
#[cfg(test)]
#[repr(C)]
pub(crate) struct EngineErrorWithMessage {
    pub(crate) etype: KernelError,
    pub(crate) message: String,
}

#[no_mangle]
pub(crate) extern "C" fn allocate_err(
    etype: KernelError,
    _: KernelStringSlice,
) -> *mut EngineError {
    let boxed = Box::new(EngineError { etype });
    Box::leak(boxed)
}

// TODO: migrate all test cases to `allocate_err_with_message` for more fine-grained assertion checks (issue#1097)
#[no_mangle]
pub(crate) extern "C" fn allocate_err_with_message(
    etype: KernelError,
    message: KernelStringSlice,
) -> *mut EngineError {
    let message = unsafe { String::try_from_slice(&message).unwrap() };
    let boxed = Box::new(EngineErrorWithMessage { etype, message });

    Box::into_raw(boxed) as *mut EngineError
}

#[no_mangle]
pub(crate) extern "C" fn allocate_str(kernel_str: KernelStringSlice) -> NullableCvoid {
    let s = unsafe { String::try_from_slice(&kernel_str) };
    let ptr = Box::into_raw(Box::new(s.unwrap())).cast(); // never null
    let ptr = unsafe { NonNull::new_unchecked(ptr) };
    Some(ptr)
}

// helper to recover an error from `allocate_str`
pub(crate) unsafe fn recover_error(ptr: *mut EngineError) -> EngineError {
    *Box::from_raw(ptr)
}

// helper to recover an error from 'allocate_str_with_message'
pub(crate) unsafe fn recover_error_with_message(ptr: *mut EngineError) -> EngineErrorWithMessage {
    *Box::from_raw(ptr as *mut EngineErrorWithMessage)
}

// helper to recover a string from `allocate_str`
pub(crate) fn recover_string(ptr: NonNull<c_void>) -> String {
    let ptr = ptr.as_ptr().cast();
    *unsafe { Box::from_raw(ptr) }
}
pub(crate) fn ok_or_panic<T>(result: ExternResult<T>) -> T {
    match result {
        ExternResult::Ok(t) => t,
        ExternResult::Err(e) => unsafe {
            panic!("Got engine error with type {:?}", (*e).etype);
        },
    }
}

pub(crate) fn assert_extern_result_error_with_message<T>(
    res: ExternResult<T>,
    expected_etype: KernelError,
    expected_message: &str,
) {
    match res {
        ExternResult::Err(e) => {
            let error = unsafe { recover_error_with_message(e) };
            assert_eq!(error.etype, expected_etype);
            assert_eq!(error.message, expected_message);
        }
        _ => panic!("Expected error of type '{expected_etype:?}' and message '{expected_message}'"),
    }
}

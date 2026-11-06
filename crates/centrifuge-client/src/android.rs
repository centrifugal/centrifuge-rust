//! Android setup for TLS certificate verification (docs/spec.md §5.2).
//!
//! On Android the OS checks server certificates through JNI, so the verifier
//! needs the JVM and an app `Context`. Call one of the `init_*` functions once,
//! **before [`ClientBuilder::build`](crate::ClientBuilder::build)**. A good
//! place is the native init your app already has, such as `JNI_OnLoad` or the
//! first call from Kotlin.
//!
//! - The setup is **process-wide**: the verifier keeps one Android runtime per
//!   process, so one call covers every client in the process.
//! - The call reaches the exact `rustls-platform-verifier` this crate links,
//!   so the app has no second copy of the verifier to keep in sync.
//! - A repeated call does nothing. So does a call after something else in the
//!   app has already set up the same verifier.
//! - **Call it even if the app already sets up `rustls-platform-verifier`
//!   another way** (directly, or through another library). This crate can't
//!   see that setup, only its own calls, and the call is then a no-op that
//!   makes no JNI calls.
//! - Without it, `build()` fails with [`Error::Configuration`](crate::Error::Configuration)
//!   if a connection known at build time uses TLS: a `wss://` or `https://`
//!   endpoint, an `https://` `emulation_endpoint` with HTTP-streaming, or an
//!   explicit `https://` proxy. A connection that only turns out to use TLS
//!   when it is attempted (an HTTPS proxy from the environment, the OS, a
//!   custom proxy function or `set_proxy`) fails that attempt before any
//!   network I/O, with an error naming this module, and is retried with
//!   backoff, so a setup call that lands after `build()` (on another thread,
//!   say) takes effect on the next attempt. The verifier never panics on its
//!   first certificate check.
//! - Not needed with [`TlsConfig::danger_accept_invalid_certs`] or a custom
//!   `rustls::ClientConfig` (feature `rustls-config`), which don't use the
//!   platform verifier.
//!
//! The app also needs the verifier's Kotlin component in its Gradle build,
//! kept by Proguard. **Its version must be identical to the
//! `rustls-platform-verifier-android` crate in your `Cargo.lock`** (0.2.0 with
//! this release), or the app can crash at runtime although everything
//! compiles. The Gradle snippet in the
//! [rustls-platform-verifier Android section](https://github.com/rustls/rustls-platform-verifier#android)
//! reads that version from `Cargo.lock`, so it stays in sync on its own;
//! a version typed into Gradle by hand doesn't.
//!
//! **Exempt from semver guarantees.** The signatures use types from `jni` and
//! `rustls-platform-verifier`, both pre-1.0, and they follow those crates. The
//! `jni` crate is re-exported as [`jni`] so the app names the same types.
//!
//! A native method called from Kotlin, e.g.
//! `external fun nativeInit(context: Context): Boolean` in `com.example.App`:
//!
//! ```ignore
//! use centrifuge::android::jni::errors::ThrowRuntimeExAndDefault;
//! use centrifuge::android::jni::objects::{JClass, JObject};
//! use centrifuge::android::jni::EnvUnowned;
//!
//! #[unsafe(no_mangle)]
//! pub extern "system" fn Java_com_example_App_nativeInit<'caller>(
//!     mut unowned_env: EnvUnowned<'caller>,
//!     _class: JClass<'caller>,
//!     context: JObject<'caller>,
//! ) -> bool {
//!     // `with_env` turns the JVM's raw env into an `Env` and catches panics,
//!     // so nothing unwinds into the JVM. A failed setup returns `false` to
//!     // Kotlin instead of aborting the app.
//!     unowned_env
//!         .with_env(|env| -> centrifuge::android::jni::errors::Result<bool> {
//!             Ok(match centrifuge::android::init_with_env(env, context) {
//!                 Ok(()) => true,
//!                 Err(e) => {
//!                     log::error!("{e}");
//!                     false
//!                 }
//!             })
//!         })
//!         .resolve::<ThrowRuntimeExAndDefault>()
//! }
//! ```
//!
//! Take the env as `EnvUnowned`, never as `Env`: `Env` isn't FFI-safe.
//!
//! [`TlsConfig::danger_accept_invalid_certs`]: crate::TlsConfig::danger_accept_invalid_certs

use std::sync::atomic::{AtomicBool, Ordering};

use jni::objects::{Global, JClassLoader, JObject};

pub use jni;
pub use rustls_platform_verifier::android::Runtime;

static INITIALIZED: AtomicBool = AtomicBool::new(false);

/// Sets up the verifier from a JNI env and an Android `Context`. The simplest
/// option when nothing else in the app keeps JVM references.
///
/// # Errors
///
/// [`Error::Configuration`](crate::Error::Configuration) if a JNI call fails
/// (for example, `context` isn't a `Context`). A Java exception that call
/// raised is cleared, since the error already reports it, so `env` stays
/// usable.
pub fn init_with_env(env: &mut jni::Env<'_>, context: JObject<'_>) -> Result<(), crate::Error> {
    if let Err(e) = rustls_platform_verifier::android::init_with_env(env, context) {
        if env.exception_check() {
            env.exception_clear();
        }
        return Err(crate::Error::Configuration(format!(
            "Android TLS setup: {e}"
        )));
    }
    INITIALIZED.store(true, Ordering::Release);
    Ok(())
}

/// Sets up the verifier from references the app already holds: the JVM, a
/// global reference to a `Context`, and one to its class loader.
pub fn init_with_refs(
    java_vm: jni::JavaVM,
    context: Global<JObject<'static>>,
    loader: Global<JClassLoader<'static>>,
) {
    rustls_platform_verifier::android::init_with_refs(java_vm, context, loader);
    INITIALIZED.store(true, Ordering::Release);
}

/// Sets up the verifier with a [`Runtime`] that serves the JVM, `Context` and
/// class loader on demand. For apps that manage those themselves.
pub fn init_with_runtime(runtime: &'static dyn Runtime) {
    rustls_platform_verifier::android::init_with_runtime(runtime);
    INITIALIZED.store(true, Ordering::Release);
}

/// Whether one of the `init_*` functions has succeeded.
pub(crate) fn initialized() -> bool {
    INITIALIZED.load(Ordering::Acquire)
}

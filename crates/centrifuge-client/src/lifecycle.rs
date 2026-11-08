//! Browser lifecycle events for the core (docs/wasm-plan.md, step 7a):
//! `online` / `offline` and `pagehide` / `pageshow` with `persisted`, on the
//! global scope. A window gets all four; a Dedicated Worker only the network
//! pair. The core decides what each means for the current state.

use centrifuge_client_core::machine::{Input, Lifecycle};
use tokio::sync::mpsc;
use wasm_bindgen::JsCast as _;
use wasm_bindgen::prelude::Closure;
use web_sys::{Event, EventTarget};

/// The listeners; removed when dropped (with the driver), and only then
/// freed, so the browser never calls a freed `Closure`.
pub(crate) struct Listeners {
    target: EventTarget,
    added: Vec<(&'static str, Handler)>,
}

type Handler = Closure<dyn FnMut(Event)>;

impl Listeners {
    /// `None` where the global scope isn't an `EventTarget`.
    pub(crate) fn attach(inputs: &mpsc::UnboundedSender<Input>) -> Option<Self> {
        let target: EventTarget = js_sys::global().dyn_into().ok()?;
        let mut added = Vec::new();
        for (name, event) in [
            ("offline", Lifecycle::Offline),
            ("online", Lifecycle::Online),
            ("pagehide", Lifecycle::PageHidden),
            ("pageshow", Lifecycle::PageShown),
        ] {
            let tx = inputs.clone();
            // Only a page entering or leaving the back/forward cache counts,
            // not a normal load or unload.
            let page = matches!(event, Lifecycle::PageHidden | Lifecycle::PageShown);
            let f = Closure::<dyn FnMut(Event)>::new(move |e: Event| {
                if page && !persisted(&e) {
                    return;
                }
                let _ = tx.send(Input::Lifecycle(event));
            });
            if target
                .add_event_listener_with_callback(name, f.as_ref().unchecked_ref())
                .is_ok()
            {
                added.push((name, f));
            }
        }
        Some(Self { target, added })
    }
}

fn persisted(e: &Event) -> bool {
    js_sys::Reflect::get(e, &"persisted".into())
        .ok()
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
}

impl Drop for Listeners {
    fn drop(&mut self) {
        for (name, f) in &self.added {
            let _ = self
                .target
                .remove_event_listener_with_callback(name, f.as_ref().unchecked_ref());
        }
    }
}

//! FFI functions to allow engines to receive log and tracing events from kernel

use core::fmt;

use delta_kernel::{DeltaResult, Error};
use tracing::{field::{Field as TracingField, Visit}, Event as TracingEvent, Subscriber};
use tracing_subscriber::{filter::LevelFilter, layer::Context, registry::LookupSpan, Layer};

use crate::{kernel_string_slice, KernelStringSlice};

#[repr(C)]
pub enum Level {
    ERROR,
    WARN,
    INFO,
    DEBUG,
    TRACE,
}

impl From<&tracing::Level> for Level {
    fn from(value: &tracing::Level) -> Self {
        match *value {
            tracing::Level::TRACE => Level::TRACE,
            tracing::Level::DEBUG => Level::DEBUG,
            tracing::Level::INFO => Level::INFO,
            tracing::Level::WARN => Level::WARN,
            tracing::Level::ERROR => Level::ERROR,
        }
    }
}

impl From<Level> for LevelFilter {
    fn from(value: Level) -> Self {
        match value {
            Level::TRACE => LevelFilter::TRACE,
            Level::DEBUG => LevelFilter::DEBUG,
            Level::INFO => LevelFilter::INFO,
            Level::WARN => LevelFilter::WARN,
            Level::ERROR => LevelFilter::ERROR,
        }
    }
}


/// An `Event` can generally be thought of a "log message". It contains all the relevant bits such
/// that an engine can generate a log message in its format
#[repr(C)]
pub struct Event {
    message: KernelStringSlice,
    level: Level,
}

pub type TracingEventFn = extern "C" fn(event: Event);

/// Enable getting called back for tracing (logging) events in the kernel. `max_level` specifies
/// that only events `<=` to the specified level should be reported.  More verbose Levels are "greater
/// than" less verbose ones. So Level::ERROR is the lowest, and Level::TRACE the highest.
#[no_mangle]
pub unsafe extern "C" fn enable_event_tracing(callback: TracingEventFn, max_level: Level) {
    setup_event_subscriber(callback, max_level);
}


// utility code below for setting up the tracing subscriber stuff


struct MessageFieldVisitor {
    message: Option<String>,
}

impl Visit for MessageFieldVisitor {
    fn record_debug(&mut self, field: &TracingField, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            self.message = Some(format!("{:?}", value));
        }
    }

    fn record_str(&mut self, field: &TracingField, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.to_string());
        }
    }
}

struct EventLayer {
    callback: TracingEventFn,
}

impl<S> Layer<S> for EventLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &TracingEvent<'_>, _context: Context<'_, S>) {
        // it would be tempting to `impl TryFrom` to convert the `TracingEvent` into an `Event`, but
        // we want to use a KernelStringSlice, so we need the extracted string to live long enough
        // for the callback which won't happen if we convert inside a `try_from` call
        let metadata = event.metadata();
        let level = metadata.level().into();
        let mut message_visitor = MessageFieldVisitor { message: None };
        event.record(&mut message_visitor);
        if let Some(message) = message_visitor.message {
            // we ignore events without a message
            //println!("Message here {msg}");
            let msg = kernel_string_slice!(message);
            let event = Event {
                message: msg,
                level,
            };
            (self.callback)(event);
        }
    }
}

fn setup_event_subscriber(callback: TracingEventFn, max_level: Level) {
    use tracing_subscriber::{registry::Registry, layer::SubscriberExt};
    let filter: LevelFilter = max_level.into();
    let event_layer = EventLayer { callback }.with_filter(filter);
    let subscriber = Registry::default().with(event_layer);
    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set global subscriber");
}


// type SharedBuffer = Arc<Mutex<Vec<u8>>>;

// struct TriggerLayer {
//     buf: SharedBuffer,
// }

// impl<S> Layer<S> for TriggerLayer
// where
//     S: Subscriber + for<'a> LookupSpan<'a>,
// {
//     fn on_event(&self, event: &TracingEvent<'_>, _context: Context<'_, S>) {
//         println!("EVENT");
//         let mut buf = self.buf.lock().unwrap();
//         let output = String::from_utf8_lossy(&buf);
//         println!("Buf is:\n{output}");
//         buf.clear();
//     }
// }

// #[derive(Default)]
// struct BufferedMessageWriter {
//      current_buffer: SharedBuffer,
// }

// impl io::Write for BufferedMessageWriter {
//     fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
//         self.current_buffer.lock().unwrap().extend_from_slice(buf);
//         Ok(buf.len())
//     }

//     fn flush(&mut self) -> io::Result<()> {
//         Ok(())
//     }
// }

// impl<'a> MakeWriter<'a> for BufferedMessageWriter {
//     type Writer = Self;

//     fn make_writer(&'a self) -> Self::Writer {
//         BufferedMessageWriter {
//             current_buffer: self.current_buffer.clone()
//         }
//     }
// }

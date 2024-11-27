//! FFI functions to allow engines to receive log and tracing events from kernel

use std::sync::{Arc, Mutex};
use std::{fmt, io};

use tracing::{
    field::{Field as TracingField, Visit},
    Event as TracingEvent, Subscriber,
};
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::{filter::LevelFilter, layer::Context, registry::LookupSpan, Layer};

use crate::{kernel_string_slice, KernelStringSlice};

/// Definitions of level verbosity. Verbose Levels are "greater than" less verbose ones. So
/// Level::ERROR is the lowest, and Level::TRACE the highest.
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
    /// The log message associated with the event
    message: KernelStringSlice,
    /// Level that the event was emitted at
    level: Level,
    /// A string that specifies in what part of the system the event occurred
    target: KernelStringSlice,
    /// source file line number where the event occurred, or 0 (zero) if unknown
    line: u32,
    /// file where the event occurred. If unknown the slice `ptr` will be null and the len will be 0
    file: KernelStringSlice,
}

pub type TracingEventFn = extern "C" fn(event: Event);

/// Enable getting called back for tracing (logging) events in the kernel. `max_level` specifies
/// that only events `<=` to the specified level should be reported.  More verbose Levels are "greater
/// than" less verbose ones. So Level::ERROR is the lowest, and Level::TRACE the highest.
///
/// [`Event`] based tracing gives an engine maximal flexibility in formatting event log
/// lines. Kernel can also format events for the engine. If this is desired call
/// [`enable_log_line_tracing`] instead of this method.
///
/// # Safety
/// Caller must pass a valid function pointer for the callback
#[no_mangle]
pub unsafe extern "C" fn enable_event_tracing(callback: TracingEventFn, max_level: Level) {
    setup_event_subscriber(callback, max_level);
}

pub type TracingLogLineFn = extern "C" fn(line: KernelStringSlice);

/// Format to use for log lines. These correspond to the formats from [`tracing_subscriber`
/// formats](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/fmt/format/index.html).
#[repr(C)]
pub enum LogLineFormat {
    /// The default formatter. This emits human-readable, single-line logs for each event that
    /// occurs, with the context displayed before the formatted representation of the event.
    /// Example:
    /// `2022-02-15T18:40:14.289898Z  INFO fmt: preparing to shave yaks number_of_yaks=3`
    FULL,
    /// A variant of the FULL formatter, optimized for short line lengths. Fields from the context
    /// are appended to the fields of the formatted event, and targets are not shown.
    /// Example:
    /// `2022-02-17T19:51:05.809287Z  INFO fmt_compact: preparing to shave yaks number_of_yaks=3`
    COMPACT,
    /// Emits excessively pretty, multi-line logs, optimized for human readability. This is
    /// primarily intended to be used in local development and debugging, or for command-line
    /// applications, where automated analysis and compact storage of logs is less of a priority
    /// than readability and visual appeal.
    /// Example:
    /// ```ignore
    ///   2022-02-15T18:44:24.535324Z  INFO fmt_pretty: preparing to shave yaks, number_of_yaks: 3
    ///   at examples/examples/fmt-pretty.rs:16 on main
    /// ```
    PRETTY,
    /// Outputs newline-delimited JSON logs. This is intended for production use with systems where
    /// structured logs are consumed as JSON by analysis and viewing tools. The JSON output is not
    /// optimized for human readability.
    /// Example:
    /// `{"timestamp":"2022-02-15T18:47:10.821315Z","level":"INFO","fields":{"message":"preparing to shave yaks","number_of_yaks":3},"target":"fmt_json"}`
    JSON,
}

/// Enable getting called back with log lines in the kernel using default settings:
/// - FULL format
/// - include ansi color
/// - include timestamps
/// - include level
/// - include target
///
/// `max_level` specifies that only logs `<=` to the specified level should be reported.  More
/// verbose Levels are "greater than" less verbose ones. So Level::ERROR is the lowest, and
/// Level::TRACE the highest.
///
/// Log lines passed to the callback will already have a newline at the end.
///
/// Log line based tracing is simple for an engine as it can just log the passed string, but does
/// not provide flexibility for an engine to format events. If the engine wants to use a specific
/// format for events it should call [`enable_event_tracing`] instead of this function.
///
/// # Safety
/// Caller must pass a valid function pointer for the callback
#[no_mangle]
pub unsafe extern "C" fn enable_log_line_tracing(callback: TracingLogLineFn, max_level: Level) {
    setup_log_line_subscriber(
        callback,
        max_level,
        LogLineFormat::FULL,
        true,
        true,
        true,
        true,
    );
}

/// Enable getting called back with log lines in the kernel. This variant allows specifying
/// formatting options for the log lines. See [`enable_log_line_tracing`] for general info on
/// getting called back for log lines. Options that can be set here:
/// - `format`: see [`LogLineFormat`]
/// - `ansi`: should the formatter use ansi escapes for color
/// - `with_time`: should the formatter include a timestamp in the log message
/// - `with_level`: should the formatter include the level in the log message
/// - `with_target`: should the formatter include what part of the system the event occurred
///
/// # Safety
/// Caller must pass a valid function pointer for the callback
#[no_mangle]
pub unsafe extern "C" fn enable_formatted_log_line_tracing(
    callback: TracingLogLineFn,
    max_level: Level,
    format: LogLineFormat,
    ansi: bool,
    with_time: bool,
    with_level: bool,
    with_target: bool,
) {
    setup_log_line_subscriber(
        callback,
        max_level,
        format,
        ansi,
        with_time,
        with_level,
        with_target,
    );
}

// utility code below for setting up the tracing subscriber for events

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
            let msg = kernel_string_slice!(message);
            let target = metadata.target();
            let target = kernel_string_slice!(target);
            let file = match metadata.file() {
                Some(file) => kernel_string_slice!(file),
                None => KernelStringSlice {
                    ptr: std::ptr::null(),
                    len: 0,
                },
            };
            let event = Event {
                message: msg,
                level,
                target,
                line: metadata.line().unwrap_or(0),
                file,
            };
            (self.callback)(event);
        }
    }
}

fn setup_event_subscriber(callback: TracingEventFn, max_level: Level) {
    use tracing_subscriber::{layer::SubscriberExt, registry::Registry};
    let filter: LevelFilter = max_level.into();
    let event_layer = EventLayer { callback }.with_filter(filter);
    let subscriber = Registry::default().with(event_layer);
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set global subscriber");
}

// utility code below for setting up the tracing subscriber for log lines

type SharedBuffer = Arc<Mutex<Vec<u8>>>;

struct TriggerLayer {
    buf: SharedBuffer,
    callback: TracingLogLineFn,
}

impl<S> Layer<S> for TriggerLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, _: &TracingEvent<'_>, _context: Context<'_, S>) {
        let mut buf = self.buf.lock().unwrap();
        let output = String::from_utf8_lossy(&buf);
        let message = kernel_string_slice!(output);
        (self.callback)(message);
        buf.clear();
    }
}

#[derive(Default)]
struct BufferedMessageWriter {
    current_buffer: SharedBuffer,
}

impl io::Write for BufferedMessageWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.current_buffer.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for BufferedMessageWriter {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        BufferedMessageWriter {
            current_buffer: self.current_buffer.clone(),
        }
    }
}

fn setup_log_line_subscriber(
    callback: TracingLogLineFn,
    max_level: Level,
    format: LogLineFormat,
    ansi: bool,
    with_time: bool,
    with_level: bool,
    with_target: bool,
) {
    use tracing_subscriber::{layer::SubscriberExt, registry::Registry};
    let buffer = Arc::new(Mutex::new(vec![]));
    let writer = BufferedMessageWriter {
        current_buffer: buffer.clone(),
    };
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(writer)
        .with_ansi(ansi)
        .with_level(with_level)
        .with_target(with_target);
    let filter: LevelFilter = max_level.into();
    let tracking_layer = TriggerLayer {
        buf: buffer.clone(),
        callback,
    };

    // This repeats some code, but avoids some insane generic wrangling if we try to abstract the
    // type of `fmt_layer` over the formatter
    macro_rules! setup_subscriber {
        ( $format_fn:ident ) => {
            if with_time {
                let fmt_layer = fmt_layer.$format_fn().with_filter(filter);
                let subscriber = Registry::default().with(fmt_layer).with(tracking_layer);
                tracing::subscriber::set_global_default(subscriber)
                    .expect("Failed to set global subscriber");
            } else {
                let fmt_layer = fmt_layer.without_time().$format_fn().with_filter(filter);
                let subscriber = Registry::default().with(fmt_layer).with(tracking_layer);
                tracing::subscriber::set_global_default(subscriber)
                    .expect("Failed to set global subscriber");
            }
        };
    }
    match format {
        LogLineFormat::FULL => {
            // can't use macro as there's no `full()` function, it's just the default
            if with_time {
                let fmt_layer = fmt_layer.with_filter(filter);
                let subscriber = Registry::default().with(fmt_layer).with(tracking_layer);
                tracing::subscriber::set_global_default(subscriber)
                    .expect("Failed to set global subscriber");
            } else {
                let fmt_layer = fmt_layer.without_time().with_filter(filter);
                let subscriber = Registry::default().with(fmt_layer).with(tracking_layer);
                tracing::subscriber::set_global_default(subscriber)
                    .expect("Failed to set global subscriber");
            }
        }
        LogLineFormat::COMPACT => {
            setup_subscriber!(compact);
        }
        LogLineFormat::PRETTY => {
            setup_subscriber!(pretty);
        }
        LogLineFormat::JSON => {
            setup_subscriber!(json);
        }
    }
}

#[cfg(test)]
mod tests {
    use tracing::info;
    use tracing_subscriber::fmt::time::FormatTime;

    use crate::TryFromStringSlice;

    use super::*;

    static MESSAGES: Mutex<Option<Vec<String>>> = Mutex::new(None);

    extern "C" fn record_callback(line: KernelStringSlice) {
        let s: &str = unsafe { TryFromStringSlice::try_from_slice(&line).unwrap() };
        let s = s.to_string();
        let mut lock = MESSAGES.lock().unwrap();
        if let Some(ref mut msgs) = *lock {
            msgs.push(s);
        }
    }

    fn setup_messages() {
        *MESSAGES.lock().unwrap() = Some(vec![]);
    }

    // get the string that we should ensure is in log messages for the time. If current time seconds
    // is >= 50, return None because the minute might roll over before we actually log which would
    // invalidate this check
    fn get_time_test_str() -> Option<String> {
        #[derive(Default)]
        struct W {
            s: String,
        }
        impl fmt::Write for W {
            fn write_str(&mut self, s: &str) -> fmt::Result {
                self.s.push_str(s);
                Ok(())
            }
        }

        let mut w = W::default();
        let mut writer = tracing_subscriber::fmt::format::Writer::new(&mut w);
        let now = tracing_subscriber::fmt::time::SystemTime;
        now.format_time(&mut writer).unwrap();
        let tstr = w.s;
        if tstr.len() < 19 {
            return None;
        }
        let secs: u32 = tstr[17..19].parse().expect("Failed to parse secs");
        if secs >= 50 {
            // risk of roll-over, don't check
            return None;
        }
        // Trim to just hours and minutes
        Some(tstr[..19].to_string())
    }

    #[test]
    fn info_logs_with_log_line_tracing() {
        setup_messages();
        unsafe {
            enable_log_line_tracing(record_callback, Level::TRACE);
        }
        let lines = ["Testing 1\n", "Another line\n"];
        let test_time_str = get_time_test_str();
        for line in lines {
            // remove final newline which will be added back by logging
            info!("{}", &line[..(line.len() - 1)]);
        }
        let lock = MESSAGES.lock().unwrap();
        if let Some(ref msgs) = *lock {
            assert_eq!(msgs.len(), lines.len());
            for (got, expect) in msgs.iter().zip(lines) {
                assert!(got.ends_with(expect));
                assert!(got.contains("INFO"));
                assert!(got.contains("delta_kernel_ffi::ffi_tracing::tests"));
                if let Some(ref tstr) = test_time_str {
                    assert!(got.contains(tstr));
                }
            }
        } else {
            panic!("Messages wasn't Some");
        }
    }
}

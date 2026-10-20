use std::alloc::{GlobalAlloc, Layout, System};
use std::error::Error;
use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::time::Instant;

use delta_kernel_memory_tracking::{AllocationSource, Charge, Context, TrackingAllocator};
use peak_alloc::PeakAlloc;

fn main() -> Result<(), Box<dyn Error>> {
    let iterations: usize = std::env::var("TRACKING_ITERATIONS")
        .unwrap_or_else(|_| "100000".to_owned())
        .parse()?;
    let repeats: usize = std::env::var("TRACKING_REPEATS")
        .unwrap_or_else(|_| "5".to_owned())
        .parse()?;
    let filter = std::env::var("TRACKING_MODE").ok();
    println!("repeat,mode,process_totals,bytes,threads,shared_context,iterations,elapsed_ns,ns_per_pair,max_context_peak_bytes");
    for repeat in 0..=repeats {
        let mut modes = ["system", "process", "header", "header_external"];
        modes.rotate_left(repeat % 4);
        for mode in modes {
            if filter.as_ref().is_some_and(|filter| filter != mode) {
                continue;
            }
            for size in [64, 4096, 65536] {
                for threads in [1, 4] {
                    for shared in [false, true] {
                        let allocator: Arc<dyn GlobalAlloc + Send + Sync> = match mode {
                            "process" => Arc::new(PeakAlloc),
                            "header" | "header_external" => Arc::new(TrackingAllocator),
                            _ => Arc::new(System),
                        };
                        let parent = Context::new(1, None);
                        let barrier = Arc::new(Barrier::new(threads + 1));
                        let mut workers = Vec::new();
                        let mut contexts = Vec::new();
                        for thread in 0..threads {
                            let context = if shared {
                                parent.clone()
                            } else {
                                Context::new(thread as u64 + 2, None)
                            };
                            contexts.push(context.clone());
                            let barrier = barrier.clone();
                            let allocator = allocator.clone();
                            workers.push(std::thread::spawn(move || {
                                let _scope = context.enter();
                                let layout = Layout::from_size_align(size, 16).ok();
                                barrier.wait();
                                if let Some(layout) = layout {
                                    for _ in 0..iterations {
                                        let pointer = unsafe { allocator.alloc(layout) };
                                        if pointer.is_null() {
                                            return false;
                                        }
                                        unsafe { pointer.write(1) };
                                        black_box(pointer);
                                        if mode == "header_external" {
                                            let arrow =
                                                Charge::capture(AllocationSource::Arrow, size);
                                            let abi = Charge::capture(AllocationSource::Abi, 80);
                                            black_box((&arrow, &abi));
                                        }
                                        unsafe { allocator.dealloc(pointer, layout) };
                                    }
                                }
                                true
                            }));
                        }
                        let start = Instant::now();
                        barrier.wait();
                        for worker in workers {
                            if !worker.join().map_err(|_| "benchmark worker panicked")? {
                                return Err("benchmark allocation failed".into());
                            }
                        }
                        let elapsed = start.elapsed().as_nanos();
                        let ns_per_pair = elapsed as f64 / (iterations * threads) as f64;
                        let max_peak = contexts
                            .iter()
                            .map(|c| c.snapshot().total.peak_bytes)
                            .max()
                            .unwrap_or(0);
                        if repeat > 0 {
                            println!("{repeat},{mode},{},{size},{threads},{shared},{iterations},{elapsed},{ns_per_pair:.2},{max_peak}",
                        cfg!(feature = "process-totals"));
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

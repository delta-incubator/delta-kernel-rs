use std::rc::Rc;
use std::cell::RefCell;
use crate::FileMeta;
use crate::path::LogPath;
use crate::{DeltaResult, Error};

/// This iterator groups Delta log files into checkpoint nodes. It handles various scenarios including:
/// - Single-part checkpoints
/// - Multi-part checkpoints
/// - Multiple checkpoints for the same version
/// - Commits without checkpoints
///
/// The iterator creates a linked list of CheckpointNodes, where each node represents a checkpoint
/// (if present) and all commits up to the next checkpoint.
///
/// Sample Delta log structures and resulting node structures:
///
/// 1. Simple scenario with single-part checkpoints:
/// Files:
/// 00000000000000000000.json
/// 00000000000000000001.json
/// 00000000000000000002.checkpoint.parquet
/// 00000000000000000003.json
/// 00000000000000000004.checkpoint.parquet
/// 00000000000000000005.json
///
/// Resulting nodes:
/// Node 1: {checkpoint_version: None, checkpoint_files: None, commits: [0.json, 1.json]}
/// Node 2: {checkpoint_version: 2, checkpoint_files: [2.checkpoint.parquet], commits: [3.json]}
/// Node 3: {checkpoint_version: 4, checkpoint_files: [4.checkpoint.parquet], commits: [5.json]}
///
/// 2. Scenario with multi-part checkpoints:
/// Files:
/// 00000000000000000000.json
/// 00000000000000000001.json
/// 00000000000000000002.checkpoint.00000000001.00000000002.parquet
/// 00000000000000000002.checkpoint.00000000002.00000000002.parquet
/// 00000000000000000003.json
/// 00000000000000000004.checkpoint.parquet
/// 00000000000000000005.json
///
/// Resulting nodes:
/// Node 1: {checkpoint_version: None, checkpoint_files: None, commits: [0.json, 1.json]}
/// Node 2: {checkpoint_version: 2, checkpoint_files: [2.checkpoint.*.parquet], multi_part: true, commits: [3.json]}
/// Node 3: {checkpoint_version: 4, checkpoint_files: [4.checkpoint.parquet], commits: [5.json]}
///
/// 3. Scenario with multiple checkpoints for the same version:
/// Files:
/// 00000000000000000000.json
/// 00000000000000000001.checkpoint.00000000001.00000000002.parquet
/// 00000000000000000001.checkpoint.00000000002.00000000002.parquet
/// 00000000000000000001.checkpoint.00000000001.00000000003.parquet
/// 00000000000000000001.checkpoint.00000000002.00000000003.parquet
/// 00000000000000000001.checkpoint.00000000003.00000000003.parquet
/// 00000000000000000002.json
///
/// Resulting node:
/// Node 1: {
///     checkpoint_version: 1,
///     checkpoint_files: [1.checkpoint.00000000001.00000000003.parquet, 1.checkpoint.00000000002.00000000003.parquet, 1.checkpoint.00000000003.00000000003.parquet],
///     other_multipart_checkpoints: [[1.checkpoint.00000000001.00000000002.parquet, 1.checkpoint.00000000002.00000000002.parquet]],
///     multi_part: true,
///     commits: [2.json]
/// }

#[derive(Clone)]
pub struct CheckpointNode {
    pub checkpoint_version: Option<u64>,
    pub checkpoint_files: Option<Vec<FileMeta>>,
    pub other_multipart_checkpoints: Vec<Vec<FileMeta>>,
    pub multi_part: bool,
    pub commits: Vec<FileMeta>,
    pub next: Option<Rc<RefCell<CheckpointNode>>>,
}

pub struct DeltaLogGroupingIterator {
    pub head: Option<Rc<RefCell<CheckpointNode>>>,
    pub current: Option<Rc<RefCell<CheckpointNode>>>,
}

impl DeltaLogGroupingIterator {
    pub fn new(files: Vec<FileMeta>, beginning: Option<bool>) -> DeltaResult<Self> {
        // Sort files by version and type (checkpoints before commits)
        let mut versioned_files: Vec<(u64, FileMeta)> = files
            .into_iter()
            .filter_map(|file| {
                let log_path = LogPath::new(&file.location);
                log_path.version.map(|v| (v, file))
            })
            .collect();

        // Sort files: first by version, then by type (checkpoints before commits)
        // This ensures that for each version, we process checkpoints before commits
        versioned_files.sort_unstable_by(|(v1, f1), (v2, f2)| {
            v1.cmp(v2).then_with(|| {
                let is_checkpoint1 = LogPath::new(&f1.location).is_checkpoint;
                let is_checkpoint2 = LogPath::new(&f2.location).is_checkpoint;
                is_checkpoint2.cmp(&is_checkpoint1) // Checkpoints before commits
            })
        });

        // Initialize variables for building the linked list of nodes
        let mut current_version: Option<u64> = None;
        let mut head_node: Option<Rc<RefCell<CheckpointNode>>> = None;
        let mut last_node: Option<Rc<RefCell<CheckpointNode>>> = None;

        // Temporary storage for building the current node
        let mut current_node = CheckpointNode {
            checkpoint_version: None,
            checkpoint_files: None,
            other_multipart_checkpoints: Vec::new(),
            multi_part: false,
            commits: Vec::new(),
            next: None,
        };

        let mut iter = versioned_files.into_iter().peekable();

        while let Some((file_version, file_meta)) = iter.next() {
            let log_path = LogPath::new(&file_meta.location);
            
            // Handle version gaps and ensure we start from version 0 if beginning is true
            match current_version {
                Some(v) if v + 1 < file_version => {
                    return Err(Error::InvalidDeltaLog(format!(
                        "Version gap detected between versions {} and {}",
                        v, file_version
                    )));
                }
                None if file_version > 0 && beginning == Some(true) => {
                    return Err(Error::InvalidDeltaLog(format!(
                        "Missing commits before version {}",
                        file_version
                    )));
                }
                _ => (),
            }

            current_version = Some(file_version);

            if log_path.is_checkpoint {
                // Finalize the current node if it contains data and start a new one
                // This happens when we encounter a new checkpoint
                if current_node.checkpoint_version.is_some() || !current_node.commits.is_empty() {
                    let new_node = Rc::new(RefCell::new(CheckpointNode {
                        checkpoint_version: current_node.checkpoint_version,
                        checkpoint_files: current_node.checkpoint_files,
                        other_multipart_checkpoints: current_node.other_multipart_checkpoints,
                        multi_part: current_node.multi_part,
                        commits: current_node.commits,
                        next: None,
                    }));

                    // Link the new node to the previous one
                    if let Some(ref last) = last_node {
                        last.borrow_mut().next = Some(Rc::clone(&new_node));
                    } else {
                        head_node = Some(Rc::clone(&new_node));
                    }

                    last_node = Some(new_node);

                    // Reset current node for the new checkpoint
                    current_node = CheckpointNode {
                        checkpoint_version: None,
                        checkpoint_files: None,
                        other_multipart_checkpoints: Vec::new(),
                        multi_part: false,
                        commits: Vec::new(),
                        next: None,
                    };
                }

                // Start a new node with this checkpoint
                current_node.checkpoint_version = Some(file_version);

                if log_path.is_multi_part_checkpoint() {
                    // Handle multi-part checkpoints
                    current_node.multi_part = true;
                    let mut parts = vec![file_meta.clone()];

                    // Extract expected number of parts
                    let (_, num_parts) = log_path.get_checkpoint_part_numbers().unwrap();

                    // Collect remaining parts of the multi-part checkpoint
                    for _ in 1..num_parts {
                        if let Some((next_version, next_file_meta)) = iter.peek() {
                            let next_log_path = LogPath::new(&next_file_meta.location);
                            if *next_version == file_version && next_log_path.is_checkpoint {
                                parts.push(next_file_meta.clone());
                                iter.next(); // Consume the iterator
                            } else {
                                return Err(Error::InvalidDeltaLog(format!(
                                    "Incomplete multi-part checkpoint at version {}",
                                    file_version
                                )));
                            }
                        } else {
                            return Err(Error::InvalidDeltaLog(format!(
                                "Incomplete multi-part checkpoint at version {}",
                                file_version
                            )));
                        }
                    }

                    current_node.checkpoint_files = Some(parts);
                } else {
                    // Handle single-part checkpoint
                    current_node.checkpoint_files = Some(vec![file_meta.clone()]);
                    current_node.multi_part = false;
                }
            } else if log_path.is_commit {
                // Add commit file to the current node
                current_node.commits.push(file_meta.clone());
            } else {
                // Skip unknown file types
                continue;
            }
        }

        // Finalize the last node
        if current_node.checkpoint_version.is_some() || !current_node.commits.is_empty() {
            let new_node = Rc::new(RefCell::new(CheckpointNode {
                checkpoint_version: current_node.checkpoint_version,
                checkpoint_files: current_node.checkpoint_files,
                other_multipart_checkpoints: current_node.other_multipart_checkpoints,
                multi_part: current_node.multi_part,
                commits: current_node.commits,
                next: None,
            }));

            if let Some(ref last) = last_node {
                last.borrow_mut().next = Some(Rc::clone(&new_node));
            } else {
                head_node = Some(Rc::clone(&new_node));
            }
        }

        Ok(DeltaLogGroupingIterator {
            head: head_node,
            current: None,
        })
    }
}

// Implement Iterator for DeltaLogGroupingIterator
impl Iterator for DeltaLogGroupingIterator {
    type Item = Rc<RefCell<CheckpointNode>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current.is_none() {
            self.current = self.head.clone();
        } else {
            let next_node = self.current.as_ref().and_then(|node| node.borrow().next.clone());
            self.current = next_node;
        }

        self.current.clone()
    }
}

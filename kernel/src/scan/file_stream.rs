use std::collections::HashSet;

use super::data_skipping::data_skipping_filter;
use crate::actions::{parse_actions, Action, ActionType, Add};
use crate::expressions::Expression;
use crate::DeltaResult;
use arrow_array::RecordBatch;
#[cfg(feature = "async")]
use futures::stream::{BoxStream, Stream};
#[cfg(feature = "async")]
use futures::StreamExt;

struct LogReplayScanner {
    predicate: Option<Expression>,
    seen: HashSet<(String, Option<String>)>,
}

impl LogReplayScanner {
    /// Create a new [`LogReplayStream`] instance
    fn new(predicate: Option<Expression>) -> Self {
        Self {
            predicate,
            seen: Default::default(),
        }
    }

    fn process_batch(&mut self, actions: RecordBatch) -> DeltaResult<Vec<Add>> {
        let actions = if let Some(predicate) = &self.predicate {
            data_skipping_filter(actions, predicate)?
        } else {
            actions
        };

        let adds: Vec<Add> = parse_actions(&actions, &[ActionType::Remove, ActionType::Add])?
            .filter_map(|action| match action {
                Action::Add(add)
                    // Note: each (add.path + add.dv_unique_id()) pair has a 
                    // unique Add + Remove pair in the log. For example:
                    // https://github.com/delta-io/delta/blob/master/spark/src/test/resources/delta/table-with-dv-large/_delta_log/00000000000000000001.json
                    if !self
                        .seen
                        .contains(&(add.path.clone(), add.dv_unique_id())) =>
                {
                    self.seen.insert((add.path.clone(), add.dv_unique_id()));
                    Some(add)
                }
                Action::Add(add) => {
                    self.seen.insert((add.path.clone(), add.dv_unique_id()));
                    None
                }
                Action::Remove(remove) => {
                    self.seen
                        .insert((remove.path.clone(), remove.dv_unique_id()));
                    None
                }
                _ => None,
            })
            .collect();

        Ok(adds)
    }
}

/// Given a stream of actions and a predicate, returns a stream of [Add]
#[cfg(feature = "async")]
pub fn log_replay_stream(
    action_stream: BoxStream<'static, DeltaResult<RecordBatch>>,
    predicate: Option<Expression>,
) -> impl Stream<Item = DeltaResult<Add>> {
    use futures::TryStreamExt;

    let log_scanner = LogReplayScanner::new(predicate);

    futures::stream::unfold(
        (action_stream, log_scanner),
        |(mut action_stream, mut log_scanner)| async move {
            let actions = match action_stream.next().await {
                Some(Ok(actions)) => actions,
                Some(Err(err)) => return Some((Err(err), (action_stream, log_scanner))),
                None => return None,
            };

            let adds = match log_scanner.process_batch(actions) {
                Ok(adds) => adds,
                Err(err) => return Some((Err(err), (action_stream, log_scanner))),
            };

            Some((Ok(adds), (action_stream, log_scanner)))
        },
    )
    .map_ok(|adds| futures::stream::iter(adds).map(Ok))
    .try_flatten()
}

/// Given an iterator of actions and a predicate, returns a stream of [Add]
#[cfg(feature = "sync")]
pub fn log_replay_iter(
    action_iter: impl Iterator<Item = DeltaResult<RecordBatch>>,
    predicate: Option<Expression>,
) -> impl Iterator<Item = DeltaResult<Add>> {
    let mut log_scanner = LogReplayScanner::new(predicate);

    action_iter
        .flat_map(
            move |actions| -> Box<dyn Iterator<Item = DeltaResult<Add>>> {
                match actions {
                    Ok(actions) => match log_scanner.process_batch(actions) {
                        Ok(adds) => Box::new(adds.into_iter().map(Ok)),
                        Err(err) => Box::new(std::iter::once(Err(err))),
                    },
                    Err(err) => Box::new(std::iter::once(Err(err))),
                }
            },
        )
}

#[cfg(test)]
mod tests {
    // TODO: add tests
}

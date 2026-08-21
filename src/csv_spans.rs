use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CsvSpan {
    pub(crate) sequence: usize,
    pub(crate) start: usize,
    pub(crate) end: usize,
    pub(crate) expected_rows: usize,
}

pub(crate) struct CsvSpanIter<B> {
    bytes: B,
    position: usize,
    sequence: usize,
    requested_rows: usize,
    target_rows: Arc<AtomicUsize>,
    target_input_bytes: usize,
    finished: bool,
}

impl<B> CsvSpanIter<B>
where
    B: AsRef<[u8]>,
{
    pub(crate) fn new(
        bytes: B,
        requested_rows: usize,
        target_rows: Arc<AtomicUsize>,
        target_input_bytes: usize,
        has_header: bool,
    ) -> Result<Self, String> {
        let position = if has_header {
            scan_record_end(bytes.as_ref(), 0)?
                .ok_or_else(|| "CSV header is missing".to_string())?
        } else {
            0
        };
        Ok(Self {
            bytes,
            position,
            sequence: 0,
            requested_rows,
            target_rows,
            target_input_bytes: target_input_bytes.max(1),
            finished: false,
        })
    }
}

impl<B> Iterator for CsvSpanIter<B>
where
    B: AsRef<[u8]>,
{
    type Item = Result<CsvSpan, String>;

    fn next(&mut self) -> Option<Self::Item> {
        let bytes = self.bytes.as_ref();
        if self.finished || self.position >= bytes.len() {
            return None;
        }

        let start = self.position;
        let row_limit = self
            .target_rows
            .load(Ordering::Relaxed)
            .clamp(1, self.requested_rows);
        let mut rows = 0usize;

        loop {
            match scan_record_end(bytes, self.position) {
                Ok(Some(end)) => {
                    rows += 1;
                    self.position = end;
                    if rows >= row_limit
                        || self.position.saturating_sub(start) >= self.target_input_bytes
                        || self.position >= bytes.len()
                    {
                        let span = CsvSpan {
                            sequence: self.sequence,
                            start,
                            end: self.position,
                            expected_rows: rows,
                        };
                        self.sequence += 1;
                        return Some(Ok(span));
                    }
                }
                Ok(None) => {
                    self.finished = true;
                    if rows == 0 {
                        return None;
                    }
                    let span = CsvSpan {
                        sequence: self.sequence,
                        start,
                        end: self.position,
                        expected_rows: rows,
                    };
                    self.sequence += 1;
                    return Some(Ok(span));
                }
                Err(error) => {
                    self.finished = true;
                    return Some(Err(error));
                }
            }
        }
    }
}

pub(crate) fn scan_record_end(bytes: &[u8], start: usize) -> Result<Option<usize>, String> {
    if start >= bytes.len() {
        return Ok(None);
    }

    let mut in_quotes = false;
    let mut index = start;
    while index < bytes.len() {
        match bytes[index] {
            b'"' if in_quotes && index + 1 < bytes.len() && bytes[index + 1] == b'"' => {
                index += 2;
                continue;
            }
            b'"' => in_quotes = !in_quotes,
            b'\n' if !in_quotes => return Ok(Some(index + 1)),
            _ => {}
        }
        index += 1;
    }

    if in_quotes {
        Err(format!(
            "unterminated quoted CSV record beginning at byte {start}"
        ))
    } else {
        Ok(Some(bytes.len()))
    }
}

#[cfg(test)]
mod tests {
    use super::{CsvSpan, CsvSpanIter};
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    fn spans(input: &str, rows: usize, bytes: usize, has_header: bool) -> Vec<CsvSpan> {
        CsvSpanIter::new(
            Arc::<[u8]>::from(input.as_bytes()),
            rows,
            Arc::new(AtomicUsize::new(rows)),
            bytes,
            has_header,
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
    }

    #[test]
    fn quoted_newline_does_not_split_a_record() {
        let input = "id,text\n1,\"first\nsecond\"\n2,last\n";
        let result = spans(input, 1, usize::MAX, true);

        assert_eq!(result.len(), 2);
        assert_eq!(
            &input.as_bytes()[result[0].start..result[0].end],
            b"1,\"first\nsecond\"\n"
        );
        assert_eq!(result[0].expected_rows, 1);
    }

    #[test]
    fn escaped_quotes_and_missing_final_newline_are_supported() {
        let input = "id,text\r\n1,\"a \"\"quoted\"\" value\"\r\n2,last";
        let result = spans(input, 1, usize::MAX, true);

        assert_eq!(result.len(), 2);
        assert_eq!(result[1].end, input.len());
    }

    #[test]
    fn input_byte_cap_is_applied_at_the_next_safe_boundary() {
        let input = "1,aaaa\n2,bbbb\n3,cccc\n";
        let result = spans(input, 10, 8, false);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].expected_rows, 2);
        assert_eq!(result[1].expected_rows, 1);
    }

    #[test]
    fn unterminated_quote_is_rejected() {
        let input = Arc::<[u8]>::from(b"1,\"broken\n".as_slice());
        let error = CsvSpanIter::new(input, 10, Arc::new(AtomicUsize::new(10)), 1024, false)
            .unwrap()
            .next()
            .unwrap()
            .unwrap_err();

        assert!(error.contains("unterminated quoted CSV record"));
    }
}

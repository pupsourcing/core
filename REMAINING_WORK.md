# Remaining Work: Self-Claiming Segments (Axon-Inspired)

## ✅ Completed

- Segment type + SegmentStore interface (`es/store/segment.go`)
- SegmentProcessorConfig + defaults (`es/consumer/segment.go`)
- PostgreSQL SegmentStore + SegmentProcessor
- MySQL SegmentStore + SegmentProcessor
- Migration generator (PG, MySQL, SQLite)
- Unit tests (config, ceilDiv, fair-share)
- Integration tests (PG + MySQL — all passing)
- Code review + all fixes applied
- Reviewer agent updated to run `make test-integration-local`

## 🔲 Remaining

### docs-examples — Documentation and scaling example

1. **Update README.md** — Add a "Scaling with Segments" section explaining:
   - What segments are and how they work
   - How to use `SegmentProcessor` instead of `Processor`
   - How to deploy multiple workers (just run the same binary N times)
   - Configuration options (TotalSegments, HeartbeatInterval, etc.)

2. **Add a scaling example** — `examples/segment-scaling/` or update `examples/scaling/`:
   - Show how to create a `SegmentProcessor`
   - Show how `RunModeOneOff` works for testing
   - Show `RunModeContinuous` for production

3. **Update `es/doc.go`** — Add segment processor to the package overview

4. **Update `pkg/pupsourcing.go`** — Add segment processor to the public API entry point docs

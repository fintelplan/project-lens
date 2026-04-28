# LENS-020 S2-F Architecture Decision — v3 (FINAL RECOMMENDATION)

**Decision**: Option B — Dual-provider Ensemble
**qwen-3-235b on Cerebras + gpt-oss-120b on Cloudflare Workers AI**
**Status**: READY FOR OPERATOR RATIFICATION
**Updated**: ~01:00 Thai, Apr 29 2026

## 2-lab matrix results
- Article 6 (steno):   qwen 5ops/0.88 | gpt-oss 5ops/0.86
- Article 7 (invest.): qwen 8ops/0.88 | gpt-oss 7ops/0.86
- Article 1 (wire):    qwen 8ops/0.88 | gpt-oss 9ops/0.93
- Article 3 (opinion): qwen 4ops/0.88 | gpt-oss 11ops/0.92

## Decision: Ensemble on dual providers
- qwen-3-235b zone: OP-024-029 (structural/apparatus)
- gpt-oss-120b zone: OP-002/003/005/008/010/011/015/016/022 (rhetorical/semantic)
- Local Ollama: ELIMINATED (CPU/iGPU timeout on catalog v3.1)
- Cost: $0/month (Cerebras free + Cloudflare Workers AI free)

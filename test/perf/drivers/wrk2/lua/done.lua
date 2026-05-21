-- 通用 wrk2 done() hook：写入扁平的 summary.json，
-- 与 run.sh eval_verdict（HTTP 分支）兼容。用于读路径 GET driver。
function done(summary, latency, requests)
    local out_dir = os.getenv("OUT_DIR")
    if not out_dir or out_dir == "" then
        io.stderr:write("done.lua: OUT_DIR not set\n")
        return
    end
    local err = summary.errors or {}
    local non2xx = (err.status or 0) + (err.timeout or 0)
                 + (err.connect or 0) + (err.read or 0) + (err.write or 0)
    local throughput = 0
    if summary.duration and summary.duration > 0 then
        throughput = summary.requests / (summary.duration / 1e6)
    end
    local p50 = latency:percentile(50.0) / 1000.0
    local p95 = latency:percentile(95.0) / 1000.0
    local p99 = latency:percentile(99.0) / 1000.0
    local f = io.open(out_dir .. "/summary.json", "w")
    f:write(string.format(
        '{"throughput":%.4f,"requests":%d,"non2xx":%d,' ..
        '"p50_ms":%.4f,"p95_ms":%.4f,"p99_ms":%.4f}\n',
        throughput, summary.requests, non2xx, p50, p95, p99))
    f:close()
end

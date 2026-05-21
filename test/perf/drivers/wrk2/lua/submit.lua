-- wrk2 Lua 脚本用于 POST /api/v1/tasks：每个请求都带唯一 `name`，
-- 以绕过服务端可能存在的 idempotency cache。done() 与 lua/done.lua 保持一致，
-- 使 run.sh eval_verdict 看到和读路径 cell 相同的 summary.json schema。

local thread_seq = 0

function setup(thread)
    thread_seq = thread_seq + 1
    thread:set("tid", thread_seq)
end

local counter = 0
local pid_tag = tostring(os.time())
local seed_queue = os.getenv("SEED_QUEUE") or "default"
local body_template =
    '{"name":"bench-%d-%s-%d","namespace":"perf","type":"example.echo",' ..
    '"queue_name":"' .. seed_queue .. '","priority":5,' ..
    '"payload":{"hello":"world"},"timeout":"30s"}'

function init(args)
    wrk.method = "POST"
    wrk.headers["Content-Type"] = "application/json"
end

function request()
    counter = counter + 1
    local body = string.format(body_template, tid or 0, pid_tag, counter)
    return wrk.format(nil, nil, nil, body)
end

function done(summary, latency, requests)
    local out_dir = os.getenv("OUT_DIR")
    if not out_dir or out_dir == "" then
        io.stderr:write("submit.lua: OUT_DIR not set\n")
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

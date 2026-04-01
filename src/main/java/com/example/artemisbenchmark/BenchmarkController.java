package com.example.artemisbenchmark;

import org.springframework.web.bind.annotation.*;
import java.util.Map;

@RestController
@RequestMapping("/api/benchmark")
public class BenchmarkController {

    private final BenchmarkEngine engine;

    public BenchmarkController(BenchmarkEngine engine) {
        this.engine = engine;
    }

    @PostMapping("/start")
    public void start(@RequestBody Map<String, String> config) {
        engine.start(config);
    }

    @PostMapping("/stop")
    public void stop() {
        engine.stop();
    }

    @GetMapping("/stats")
    public Map<String, Object> getStats() {
        return engine.getStats();
    }
}
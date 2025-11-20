package com.erfan.fpl_ai_recommender.api;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestController {
    /**
     * dummy doc
     * @return
     */
    @GetMapping("/api/test")
    public String test() {
        return "Backend is running!";
    }
}

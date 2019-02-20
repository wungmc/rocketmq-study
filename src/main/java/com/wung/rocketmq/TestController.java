/*
 * Copyright (C), 2011-2019.
 */
package com.wung.rocketmq;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 *
 * @author wung 2019/2/20.
 */
@RestController
public class TestController {
	
	@RequestMapping("/")
	public String index() {
		return "hello world";
	}
	
}

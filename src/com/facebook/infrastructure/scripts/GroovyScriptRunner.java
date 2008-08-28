package com.facebook.infrastructure.scripts;

import groovy.lang.GroovyShell;

public class GroovyScriptRunner
{
	private static GroovyShell groovyShell_ = new GroovyShell();

	public static String evaluateString(String script)
	{
		 return groovyShell_.evaluate(script).toString();
	}
}

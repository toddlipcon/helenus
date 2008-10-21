/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.facebook.infrastructure.test;

import com.facebook.infrastructure.service.StorageService;
import com.facebook.infrastructure.utils.LogUtil;

/**
 *
 * @author kranganathan
 */
public class testStorage
{
    public static void main(String[] args)
    {
    	try
    	{
	        LogUtil.init();
	        StorageService s = StorageService.instance();
	        s.start();

	        testMultipleKeys.testCompactions();
    	}
    	catch (Throwable t)
    	{
    		t.printStackTrace();
    	}
    }
}

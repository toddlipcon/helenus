/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.facebook.infrastructure.test;

import java.util.Random;

import com.facebook.infrastructure.utils.LogUtil;
import com.facebook.infrastructure.config.DatabaseDescriptor;
import com.facebook.infrastructure.db.ColumnFamily;
import com.facebook.infrastructure.db.IColumn;
import com.facebook.infrastructure.db.RowMutation;
import com.facebook.infrastructure.db.Table;
import com.facebook.infrastructure.service.*;

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

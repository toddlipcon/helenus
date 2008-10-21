package com.facebook.infrastructure.locator;

import static org.junit.Assert.*;

import java.net.UnknownHostException;

import org.junit.Test;

import com.facebook.infrastructure.net.EndPoint;

public class EndPointSnitchTest
{

    @Test
    public void test() throws UnknownHostException
    {
        EndPoint endPoint1 = new EndPoint("192.168.0.1", 1234);
        EndPoint endPoint2 = new EndPoint("192.168.0.2", 1234);
        EndPoint endPoint3 = new EndPoint("192.168.100.2", 1234);
        EndPoint endPoint4 = new EndPoint("123.123.123.2", 1234);
        
        EndPointSnitch snitch = new EndPointSnitch();
        assertTrue(snitch.isOnSameRack(endPoint1, endPoint2));
        assertFalse(snitch.isOnSameRack(endPoint1, endPoint3));
        
        assertTrue(snitch.isInSameDataCenter(endPoint1, endPoint2));
        assertTrue(snitch.isInSameDataCenter(endPoint1, endPoint3));
        assertFalse(snitch.isInSameDataCenter(endPoint1, endPoint4));
    }

}

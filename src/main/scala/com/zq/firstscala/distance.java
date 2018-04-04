package com.zq.firstscala;


public class distance{
	public static double evaluate(double latA, double logA, double latB, double logB) {
		int earthR = 6371000;
		double x = Math.cos(latA * Math.PI / 180) * Math.cos(latB * Math.PI / 180)
				* Math.cos((logA - logB) * Math.PI / 180);
		double y = Math.sin(latA * Math.PI / 180) * Math.sin(latB * Math.PI / 180);
		double s = x + y;
		if (s > 1)
			s = 1;
		if (s < -1)
			s = -1;
		double alpha = Math.acos(s);
		double distance = alpha * earthR;
		return distance;
	}
}
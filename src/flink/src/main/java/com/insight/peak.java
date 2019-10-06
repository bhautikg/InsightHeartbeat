package com.insight;

//https://github.com/JorenSix/TarsosDSP/blob/master/src/core/be/tarsos/dsp/beatroot/Peaks.java
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.IntStream;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class peak {


    public static boolean debug = false;
    public static int pre = 3;
    public static int post = 1;

    /**
     * General peak picking method for finding n local maxima in an array
     *  @param data input data
     *  @param peaks list of peak indexes
     *  @param width minimum distance between peaks
     *  @return The number of peaks found
     */
    public static int findPeaks(double[] data, int[] peaks, int width) {
        int peakCount = 0;
        int maxp = 0;
        int mid = 0;
        int end = data.length;
        while (mid < end) {
            int i = mid - width;
            if (i < 0)
                i = 0;
            int stop = mid + width + 1;
            if (stop > data.length)
                stop = data.length;
            maxp = i;
            for (i++; i < stop; i++)
                if (data[i] > data[maxp])
                    maxp = i;
            if (maxp == mid) {
                int j;
                for (j = peakCount; j > 0; j--) {
                    if (data[maxp] <= data[peaks[j-1]])
                        break;
                    else if (j < peaks.length)
                        peaks[j] = peaks[j-1];
                }
                if (j != peaks.length)
                    peaks[j] = maxp;
                if (peakCount != peaks.length)
                    peakCount++;
            }
            mid++;
        }
        return peakCount;
    } // findPeaks()

    /** General peak picking method for finding local maxima in an array
     *  @param data input data
     *  @param width minimum distance between peaks
     *  @param threshold minimum value of peaks
     *  @return list of peak indexes
     */
    public static LinkedList<Integer> findPeaks(double[] data, int width,
                                                double threshold) {
        return findPeaks(data, width, threshold, 0, false);
    } // findPeaks()

    /** General peak picking method for finding local maxima in an array
     *  @param data input data
     *  @param width minimum distance between peaks
     *  @param threshold minimum value of peaks
     *  @param decayRate how quickly previous peaks are forgotten
     *  @param isRelative minimum value of peaks is relative to local average
     *  @return list of peak indexes
     */
    public static LinkedList<Integer> findPeaks(double[] data, int width,
                                                double threshold, double decayRate, boolean isRelative) {
        LinkedList<Integer> peaks = new LinkedList<Integer>();
        int maxp = 0;
        int mid = 0;
        int end = data.length;
        double av = data[0];
        while (mid < end) {
            av = decayRate * av + (1 - decayRate) * data[mid];
            if (av < data[mid])
                av = data[mid];
            int i = mid - width;
            if (i < 0)
                i = 0;
            int stop = mid + width + 1;
            if (stop > data.length)
                stop = data.length;
            maxp = i;
            for (i++; i < stop; i++)
                if (data[i] > data[maxp])
                    maxp = i;
            if (maxp == mid) {
                if (overThreshold(data, maxp, width, threshold, isRelative,av)){
                    if (debug)
                        System.out.println(" peak");
                    peaks.add(new Integer(maxp));
                } else if (debug)
                    System.out.println();
            }
            mid++;
        }
        return peaks;
    } // findPeaks()

    public static double expDecayWithHold(double av, double decayRate,
                                          double[] data, int start, int stop) {
        while (start < stop) {
            av = decayRate * av + (1 - decayRate) * data[start];
            if (av < data[start])
                av = data[start];
            start++;
        }
        return av;
    } // expDecayWithHold()

    public static boolean overThreshold(double[] data, int index, int width,
                                        double threshold, boolean isRelative,
                                        double av) {
        if (debug)
            System.out.printf("%4d : %6.3f     Av1: %6.3f    ",
                    index, data[index], av);
        if (data[index] < av)
            return false;
        if (isRelative) {
            int iStart = index - pre * width;
            if (iStart < 0)
                iStart = 0;
            int iStop = index + post * width;
            if (iStop > data.length)
                iStop = data.length;
            double sum = 0;
            int count = iStop - iStart;
            while (iStart < iStop)
                sum += data[iStart++];
            if (debug)
                System.out.printf("    %6.3f    %6.3f   ", sum / count,
                        data[index] - sum / count - threshold);
            return (data[index] > sum / count + threshold);
        } else
            return (data[index] > threshold);
    } // overThreshold()

    public static void normalise(double[] data) {
        double sx = 0;
        double sxx = 0;
        for (int i = 0; i < data.length; i++) {
            sx += data[i];
            sxx += data[i] * data[i];
        }
        double mean = sx / data.length;
        double sd = Math.sqrt((sxx - sx * mean) / data.length);
        if (sd == 0)
            sd = 1;		// all data[i] == mean  -> 0; avoids div by 0
        for (int i = 0; i < data.length; i++) {
            data[i] = (data[i] - mean) / sd;
        }
    } // normalise()

    /** Uses an n-point linear regression to estimate the slope of data.
     *  @param data input data
     *  @param hop spacing of data points
     *  @param n length of linear regression
     *  @param slope output data
     */
    public static void getSlope(double[] data, double hop, int n,
                                double[] slope) {
        int i = 0, j = 0;
        double t;
        double sx = 0, sxx = 0, sy = 0, sxy = 0;
        for ( ; i < n; i++) {
            t = i * hop;
            sx += t;
            sxx += t * t;
            sy += data[i];
            sxy += t * data[i];
        }
        double delta = n * sxx - sx * sx;
        for ( ; j < n / 2; j++)
            slope[j] = (n * sxy - sx * sy) / delta;
        for ( ; j < data.length - (n + 1) / 2; j++, i++) {
            slope[j] = (n * sxy - sx * sy) / delta;
            sy += data[i] - data[i - n];
            sxy += hop * (n * data[i] - sy);
        }
        for ( ; j < data.length; j++)
            slope[j] = (n * sxy - sx * sy) / delta;
    } // getSlope()

    public static double min(double[] arr) { return arr[imin(arr)]; }

    public static double max(double[] arr) { return arr[imax(arr)]; }

    public static int imin(double[] arr) {
        int i = 0;
        for (int j = 1; j < arr.length; j++)
            if (arr[j] < arr[i])
                i = j;
        return i;
    } // imin()

    public static int imax(double[] arr) {
        int i = 0;
        for (int j = 1; j < arr.length; j++)
            if (arr[j] > arr[i])
                i = j;
        return i;
    } // imax()



    public static void mainfdfdsf(String[] args) throws FileNotFoundException, IOException {
        // TODO Auto-generated method stub

        double[] ecg_data = ecgData2.sig;
//        try (BufferedReader br = new BufferedReader(new FileReader("forJava.csv"))) {
//            String line;
//            while ((line = br.readLine()) != null) {
//                String[] values = line.split(",");
//                records.add(Double.parseDouble(values[0]));
//            }
//            double[] ecg_data = new double[records.size()];
//
//            for (int i = 0; i < records.size(); i++)
//            {
//                ecg_data[i] = records.get(i);
//            }
            //peak p1 = new peak();
            LinkedList<Integer> peaks = peak.findPeaks(ecg_data, 50, 0.75);
            System.out.println(Arrays.toString(peaks.toArray()));
            System.out.println(ecg_data[266]);
            double[] ANE = new double[peaks.size()-1];
            double[] interval = new double[ANE.length];

            for(int i=0;i<peaks.size()-1 ;i++){
                int start = peaks.get(i);
                int end = peaks.get(i+1);
                double[] slice = Arrays.copyOfRange(ecg_data, start, end + 1);

                double[] NE = new double[slice.length];

                for(int j=0;j<slice.length-2; j++) {
                    NE[j]= slice[j+1]*slice[j+1] - slice[j]* slice[j+2];
                }
                ANE[i] = Arrays.stream(NE).sum()/NE.length;
                interval[i] = NE.length/360.0;

            } // for block
            System.out.println(Arrays.toString(ANE));
            System.out.println(Arrays.toString(interval));

        } //Try block
    } // main block

 // class Peaks
 class ecgData2 {
     //public final static Tuple2<String, double[]>  data= new Tuple2<>("mgh002", ecgData.sig);
     public final static double[] sig = new double[] {
             0.05, 0.05, 0.05, 0.05, 0.05,0.05,0.05,0.05,0.05,0.035,
             0.035,0.05, 0.065,0.065,
             0.04,
             0.045,
             0.035,
             0.055,
             0.07,
             0.075,
             0.065,
             0.055,
             0.07,
             0.08,
             0.09,
             0.085,
             0.09,
             0.085,
             0.085,
             0.105,
             0.12,
             0.125,
             0.12,
             0.125,
             0.135,
             0.145,
             0.16,
             0.165,
             0.175,
             0.17,
             0.18,
             0.21,
             0.23,
             0.235,
             0.24,
             0.255,
             0.255,
             0.275,
             0.3,
             0.295,
             0.315,
             0.315,
             0.335,
             0.35,
             0.35,
             0.355,
             0.355,
             0.35,
             0.355,
             0.37,
             0.39,
             0.395,
             0.385,
             0.37,
             0.365,
             0.38,
             0.385,
             0.375,
             0.37,
             0.345,
             0.345,
             0.335,
             0.33,
             0.31,
             0.28,
             0.265,
             0.25,
             0.24,
             0.225,
             0.205,
             0.18,
             0.165,
             0.145,
             0.135,
             0.13,
             0.125,
             0.105,
             0.085,
             0.075,
             0.08,
             0.08,
             0.075,
             0.06,
             0.04,
             0.04,
             0.05,
             0.065,
             0.05,
             0.04,
             0.03,
             0.03,
             0.035,
             0.045,
             0.045,
             0.035,
             0.025,
             0.015,
             0.02,
             0.035,
             0.045,
             0.045,
             0.04,
             0.025,
             0.045,
             0.06,
             0.06,
             0.045,
             0.04,
             0.05,
             0.055,
             0.07,
             0.07,
             0.07,
             0.05,
             0.055,
             0.065,
             0.065,
             0.075,
             0.065,
             0.06,
             0.065,
             0.07,
             0.09,
             0.07,
             0.06,
             0.065,
             0.055,
             0.065,
             0.065,
             0.08,
             0.075,
             0.07,
             0.06,
             0.06,
             0.065,
             0.07,
             0.05,
             0.045,
             0.045,
             0.05,
             0.06,
             0.05,
             0.04,
             0.03,
             0.02,
             0.02,
             0.04,
             0.025,
             0.025,
             0.025,
             0.01,
             0.015,
             0.03,
             0.01,
             0.00,
             -0.01,
             -0.01,
             -0.005,
             0.005,
             0.00,
             -0.005,
             -0.02,
             -0.02,
             -0.01,
             -0.01,
             -0.01,
             -0.025,
             -0.035,
             -0.03,
             -0.02,
             -0.015,
             -0.015,
             -0.03,
             -0.045,
             -0.04,
             -0.04,
             -0.02,
             -0.035,
             -0.03,
             -0.045,
             -0.04,
             -0.025,
             -0.015,
             -0.015,
             -0.025,
             -0.035,
             -0.03,
             -0.015,
             -0.005,
             -0.01,
             -0.02,
             -0.03,
             -0.025,
             -0.02,
             -0.01,
             -0.02,
             -0.015,
             -0.035,
             -0.04,
             -0.04,
             -0.04,
             -0.06,
             -0.07,
             -0.085,
             -0.1,
             -0.09,
             -0.055,
             -0.055,
             -0.035,
             -0.035,
             -0.045,
             -0.025,
             -0.03,
             -0.035,
             -0.045,
             -0.07,
             -0.07,
             -0.06,
             -0.05,
             -0.055,
             -0.065,
             -0.075,
             -0.085,
             -0.075,
             -0.065,
             -0.075,
             -0.08,
             -0.08,
             -0.08,
             -0.065,
             -0.06,
             -0.06,
             -0.065,
             -0.06,
             -0.065,
             -0.06,
             -0.05,
             -0.05,
             -0.06,
             -0.075,
             -0.07,
             -0.055,
             -0.055,
             -0.085,
             -0.105,
             -0.11,
             -0.075,
             -0.035,
             0.01,
             0.095,
             0.235,
             0.43,
             0.67,
             0.91,
             1.16,
             1.365,
             1.435,
             1.38,
             1.17,
             0.82,
             0.38,
             -0.03
             , -0.345
             , -0.475
             , -0.48
             , -0.41
             , -0.3
             , -0.195
             , -0.1
             , -0.04
             , -0.02
             , -0.01
             , -0.02
             , -0.035
             , -0.04
             , -0.045
             , -0.05
             , -0.045
             , -0.04
             , -0.055
             , -0.06
             , -0.06
             , -0.075
             , -0.055
             , -0.05
             , -0.05
             , -0.06
             , -0.06
             , -0.06
             , -0.05
             , -0.045
             , -0.055
             , -0.065
             , -0.06
             , -0.055
             , -0.045
             , -0.03
             , -0.045
             , -0.04
             , -0.05
             , -0.05, -0.035, -0.025, -0.02, -0.04, -0.04, -0.04, -0.035, -0.025, -0.02,
             -0.02, -0.02, -0.02, -0.005, 0.005, -0.005, 0.00, -0.005, 0.00, 0.005, 0.035, 0.03, 0.035, 0.025,
             0.03, 0.055, 0.08, 0.06, 0.065, 0.075, 0.08, 0.11, 0.14, 0.135, 0.15, 0.155,
             0.155, 0.18, 0.2, 0.205, 0.21, 0.22, 0.225, 0.24, 0.265, 0.275,
             0.26, 0.27, 0.275, 0.28, 0.3, 0.3, 0.3, 0.3, 0.295,
             0.305, 0.32, 0.295, 0.285, 0.275, 0.26, 0.26, 0.26, 0.245,
             0.22, 0.205, 0.19, 0.17, 0.17, 0.14,
             0.115, 0.1, 0.085, 0.065, 0.07, 0.04, 0.03, 0.01, -0.01, 0.005, 0.05,
             0.05, 0.05, 0.05, 0.05,0.05,0.05,0.05,0.05,0.035,
             0.035,0.05, 0.065,0.065,
             0.04,
             0.045,
             0.035,
             0.055,
             0.07,
             0.075,
             0.065,
             0.055,
             0.07,
             0.08,
             0.09,
             0.085,
             0.09,
             0.085,
             0.085,
             0.105,
             0.12,
             0.125,
             0.12,
             0.125,
             0.135,
             0.145,
             0.16,
             0.165,
             0.175,
             0.17,
             0.18,
             0.21,
             0.23,
             0.235,
             0.24,
             0.255,
             0.255,
             0.275,
             0.3,
             0.295,
             0.315,
             0.315,
             0.335,
             0.35,
             0.35,
             0.355,
             0.355,
             0.35,
             0.355,
             0.37,
             0.39,
             0.395,
             0.385,
             0.37,
             0.365,
             0.38,
             0.385,
             0.375,
             0.37,
             0.345,
             0.345,
             0.335,
             0.33,
             0.31,
             0.28,
             0.265,
             0.25,
             0.24,
             0.225,
             0.205,
             0.18,
             0.165,
             0.145,
             0.135,
             0.13,
             0.125,
             0.105,
             0.085,
             0.075,
             0.08,
             0.08,
             0.075,
             0.06,
             0.04,
             0.04,
             0.05,
             0.065,
             0.05,
             0.04,
             0.03,
             0.03,
             0.035,
             0.045,
             0.045,
             0.035,
             0.025,
             0.015,
             0.02,
             0.035,
             0.045,
             0.045,
             0.04,
             0.025,
             0.045,
             0.06,
             0.06,
             0.045,
             0.04,
             0.05,
             0.055,
             0.07,
             0.07,
             0.07,
             0.05,
             0.055,
             0.065,
             0.065,
             0.075,
             0.065,
             0.06,
             0.065,
             0.07,
             0.09,
             0.07,
             0.06,
             0.065,
             0.055,
             0.065,
             0.065,
             0.08,
             0.075,
             0.07,
             0.06,
             0.06,
             0.065,
             0.07,
             0.05,
             0.045,
             0.045,
             0.05,
             0.06,
             0.05,
             0.04,
             0.03,
             0.02,
             0.02,
             0.04,
             0.025,
             0.025,
             0.025,
             0.01,
             0.015,
             0.03,
             0.01,
             0.00,
             -0.01,
             -0.01,
             -0.005,
             0.005,
             0.00,
             -0.005,
             -0.02,
             -0.02,
             -0.01,
             -0.01,
             -0.01,
             -0.025,
             -0.035,
             -0.03,
             -0.02,
             -0.015,
             -0.015,
             -0.03,
             -0.045,
             -0.04,
             -0.04,
             -0.02,
             -0.035,
             -0.03,
             -0.045,
             -0.04,
             -0.025,
             -0.015,
             -0.015,
             -0.025,
             -0.035,
             -0.03,
             -0.015,
             -0.005,
             -0.01,
             -0.02,
             -0.03,
             -0.025,
             -0.02,
             -0.01,
             -0.02,
             -0.015,
             -0.035,
             -0.04,
             -0.04,
             -0.04,
             -0.06,
             -0.07,
             -0.085,
             -0.1,
             -0.09,
             -0.055,
             -0.055,
             -0.035,
             -0.035,
             -0.045,
             -0.025,
             -0.03,
             -0.035,
             -0.045,
             -0.07,
             -0.07,
             -0.06,
             -0.05,
             -0.055,
             -0.065,
             -0.075,
             -0.085,
             -0.075,
             -0.065,
             -0.075,
             -0.08,
             -0.08,
             -0.08,
             -0.065,
             -0.06,
             -0.06,
             -0.065,
             -0.06,
             -0.065,
             -0.06,
             -0.05,
             -0.05,
             -0.06,
             -0.075,
             -0.07,
             -0.055,
             -0.055,
             -0.085,
             -0.105,
             -0.11,
             -0.075,
             -0.035,
             0.01,
             0.095,
             0.235,
             0.43,
             0.67,
             0.91,
             1.16,
             1.365,
             1.435,
             1.38,
             1.17,
             0.82,
             0.38,
             -0.03
             , -0.345
             , -0.475
             , -0.48
             , -0.41
             , -0.3
             , -0.195
             , -0.1
             , -0.04
             , -0.02
             , -0.01
             , -0.02
             , -0.035
             , -0.04
             , -0.045
             , -0.05
             , -0.045
             , -0.04
             , -0.055
             , -0.06
             , -0.06
             , -0.075
             , -0.055
             , -0.05
             , -0.05
             , -0.06
             , -0.06
             , -0.06
             , -0.05
             , -0.045
             , -0.055
             , -0.065
             , -0.06
             , -0.055
             , -0.045
             , -0.03
             , -0.045
             , -0.04
             , -0.05
             , -0.05, -0.035, -0.025, -0.02, -0.04, -0.04, -0.04, -0.035, -0.025, -0.02,
             -0.02, -0.02, -0.02, -0.005, 0.005, -0.005, 0.00, -0.005, 0.00, 0.005, 0.035, 0.03, 0.035, 0.025,
             0.03, 0.055, 0.08, 0.06, 0.065, 0.075, 0.08, 0.11, 0.14, 0.135, 0.15, 0.155,
             0.155, 0.18, 0.2, 0.205, 0.21, 0.22, 0.225, 0.24, 0.265, 0.275,
             0.26, 0.27, 0.275, 0.28, 0.3, 0.3, 0.3, 0.3, 0.295,
             0.305, 0.32, 0.295, 0.285, 0.275, 0.26, 0.26, 0.26, 0.245,
             0.22, 0.205, 0.19, 0.17, 0.17, 0.14,
             0.115, 0.1, 0.085, 0.065, 0.07, 0.04, 0.03, 0.01, -0.01, 0.005};
 }
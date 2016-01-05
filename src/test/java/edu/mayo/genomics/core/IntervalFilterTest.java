package edu.mayo.genomics.core;

import static org.junit.Assert.*;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import edu.mayo.genomics.model.SimpleInterval;
import htsjdk.samtools.util.Locatable;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class IntervalFilterTest {


    public ArrayList<Locatable> input(){
        ArrayList<Locatable> input = Lists.newArrayList(
                new SimpleInterval("1",10,100)
        );
        return input;
    }

    public Object[][] intervals(){
        ArrayList<Locatable> input = input();
        ArrayList<Locatable> empty = new ArrayList<>();
        ArrayList<Locatable> manyOverlapping = Lists.newArrayList(
                new SimpleInterval("1",10,100),
                // special case: multiple intervals starting at the same place
                new SimpleInterval("1",20,50),
                new SimpleInterval("1",20,50),
                new SimpleInterval("1",20,50)
        );
        ArrayList<Locatable> mixInput = Lists.newArrayList(
                // ends before query interval
                new SimpleInterval("1",10,20),
                // ends in query interval
                new SimpleInterval("1",10,60),
                // equal to query interval
                new SimpleInterval("1",30,50),
                // covered by query interval
                new SimpleInterval("1",40,42),
                // ends after query interval
                new SimpleInterval("1",45,60),
                // starts after query interval
                new SimpleInterval("1",60,100)
        );
        ArrayList<Locatable> mixExpected = Lists.newArrayList(
                // ends in query interval
                new SimpleInterval("1",10,60),
                // equal to query interval
                new SimpleInterval("1",30,50),
                // covered by query interval
                new SimpleInterval("1",40,42),
                // ends after query interval
                new SimpleInterval("1",45,60)
        );
        // returns input single SimpleInterval, query range, expected SimpleInterval
        return new Object[][]{
                // single-point boundary cases
                new Object[]{input, new SimpleInterval("1", 10, 10), input},
                new Object[]{input, new SimpleInterval("1", 100, 100), input},
                new Object[]{input, new SimpleInterval("1", 9, 9), empty},
                new Object[]{input, new SimpleInterval("1", 11, 11), input},
                new Object[]{input, new SimpleInterval("1", 99, 99), input},
                new Object[]{input, new SimpleInterval("1", 101, 101), empty},
                // empty list boundary case
                new Object[]{empty, new SimpleInterval("1", 101, 101), empty},
                // input exactly matches the query interval
                new Object[]{input, new SimpleInterval("1", 10, 100), input},
                // multiple intervals in the same place (potential edge case for indexing)
                new Object[]{manyOverlapping, new SimpleInterval("1", 20, 20), manyOverlapping},
                // input with multiple intervals
                new Object[]{mixInput, new SimpleInterval("1",30,50), mixExpected}
        };
    }

    @Test
    public void testOverlap() throws Exception {
        Object[][] intervals = intervals();
        for(Object[] test : intervals){
            ArrayList<Locatable> input = (ArrayList<Locatable>) test[0];
            SimpleInterval query = (SimpleInterval) test[1];
            ArrayList<Locatable> expected = (ArrayList<Locatable>) test[2];
            //make the query...
            IntervalFilter<Locatable> ints = new IntervalFilter<>(input);
            ArrayList<Locatable> actual = ints.getOverlapping(query);
            Assert.assertEquals(
                    actual,
                    expected
            );

        }
    }

    @Test
    public void testManyIntervals() throws Exception {
        System.out.println("testManyIntervals...");
        ArrayList<Locatable> si = new ArrayList<>();
        final int MAX = 10_000_000;
        for (int start = 1; start<MAX; start += 100) {
            si.add(new SimpleInterval("1",start,start+10));
            si.add(new SimpleInterval("1",start,start+200));
        }

        Stopwatch indexing = Stopwatch.createStarted();
        IntervalFilter<Locatable> ints = new IntervalFilter<>(si);
        indexing.stop();

        Stopwatch v1 = Stopwatch.createStarted();
        for (int start = 101; start<MAX; start += 5000) {
            SimpleInterval interval = new SimpleInterval("1", start + 10, start + 11);
            ArrayList<Locatable> actual = ints.getOverlappingIgnoringIndex(interval);
            Assert.assertEquals(actual.size(), 3);
            // the two that start from "start", plus the long one that starts from start-100.
            // the one that starts from start-200 ends before our test point.
            for (Locatable l : actual) {
                Assert.assertTrue(interval.overlaps(l));
            }
        }
        v1.stop();
        Stopwatch v2 = Stopwatch.createStarted();
        for (int start = 101; start<MAX; start += 5000) {
            SimpleInterval interval = new SimpleInterval("1", start + 10, start + 11);
            ArrayList<Locatable> actual = ints.getOverlapping(interval);
            Assert.assertEquals(actual.size(), 3);
            // the two that start from "start", plus the long one that starts from start-100.
            // the one that starts from start-200 ends before our test point.
            for (Locatable l : actual) {
                Assert.assertTrue(interval.overlaps(l));
            }
        }
        v2.stop();

        System.out.println("non-indexed took " + v1.elapsed(TimeUnit.MILLISECONDS) + " ms, "
                + " indexed took " + v2.elapsed(TimeUnit.MILLISECONDS) + " ms, plus " + indexing.elapsed(TimeUnit.MILLISECONDS) + " for sorting&indexing.");
    }

}
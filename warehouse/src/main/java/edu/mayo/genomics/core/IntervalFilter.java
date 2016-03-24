package edu.mayo.genomics.core;

import edu.mayo.genomics.model.SimpleInterval;

import edu.mayo.genomics.utils.hellbender.GATKException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import htsjdk.samtools.util.Locatable;

import com.google.common.collect.Lists;

/**
 * Created by m102417 on 12/22/15.
 *
 * Shamelessly stolen and modified from:
 * https://github.com/broadinstitute/gatk/pull/892/files
 *
 * This is a core bit of functionality, it allows us to do genomic position based queries on flat files on hadoop.
 *
 * Holds many intervals in memory, with an efficient operation to get
 * intervals that overlap a given query interval.
 *
 * This version assumes that all the intervals lie on the same contig.
 *
 */
public class IntervalFilter<T extends Locatable> implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final Logger logger = LogManager.getLogger(IntervalFilter.class);

    // approx number of buckets we're aiming for.
    private static final int NUMBUCKETS = 1000;

    // each bucket contains 2**shift entries.
    private final int shift;

    // input intervals, sorted by start location
    private final List<T> vs;
    // the contig (landmark) all the intervals are in.
    private final String contig;

    // reach: bucket# -> how far that bucket reaches.
    // e.g. bucket 0 contains the first 2**shift locatables. reach[0] is the max over their .getEnd()
    //      reach[x] is the max over the .getEnd for that bucket and all the ones before it.
    private int[] reach;

    /**
    * Creates an IntervalsSkipList that holds a copy of the given intervals, sorted
    * and indexed.
    *
    * @param loc Locatables, not necessarily sorted. Will be iterated over exactly once.
    */
    public IntervalFilter(Iterable<T> loc) {
        vs = Lists.newArrayList(loc);
        if (vs.isEmpty()) {
            contig="";
        } else {
            contig=vs.get(0).getContig();
        }
        int bSize = vs.size() / NUMBUCKETS;
        // heuristic: if we have too many small buckets then we're better off instead
        // taking fewer but bigger steps, and then iterating through a few values.
        // Thus, put a lower bound on bucket size.
        if (bSize<32) bSize=32;
        shift = floorLog2(bSize);
        sortVariants();
        buildIndexAndCheck();
    }

    // build index and check everyone's in the same contig
    private void buildIndexAndCheck()  {
        int max = 0;
        int key = 0;
        int idx = 0;
        // reach: bucket# -> how far that bucket reaches
        reach = new int[(vs.size()>>shift)+1];
        for (Locatable v : vs) {
            if (!contig.equals(v.getContig())) {
                throw new GATKException("All intervals in IntervalsSkipList should have the same contig, but found both '"+v.getContig()+"' and '"+contig+"'.");
            }
            int k = idx>>shift;
            if (k>key) {
                reach[key]=max;
                key=k;
            }
            if (v.getEnd()>max) {
                max=v.getEnd();
            }
            idx++;
        }
        reach[key]=max;
    }

    /**
     * Returns all the intervals that overlap with the query.
     * The query doesn't *have* to be in the same contig as the intervals we
     * hold, but of course if it isn't you'll get an empty result.
     * You may modify the returned list.
     */
    public ArrayList<T> getOverlapping(SimpleInterval query) {
        if (!contig.equals(query.getContig())) {
            // different contig, so we know no one'll overlap.
            return new ArrayList<T>();
        }
        ArrayList<T> ret = new ArrayList<T>();
        // use index to skip early non-overlapping entries.
        int idx = firstPotentiallyReaching(query.getStart());
        if (idx<0) {
            idx=0;
        }
        for (;idx<vs.size();idx++) {
            T v = vs.get(idx);
            // they are sorted by start location, so if this one starts too late
            // then all of the others will, too.
            if (v.getStart() > query.getEnd()) {
                break;
            }
            if (query.overlaps(v)) {
                ret.add(v);
            }
        }
        return ret;
    }

    // returns all the intervals that overlap with the query.
    // (use the optimized version instead, unless you're testing it and need something to compare against)
    protected ArrayList<T> getOverlappingIgnoringIndex(SimpleInterval query) {
        if (!contig.equals(query.getContig())) {
            // different contig, so we know no one'll overlap.
            return new ArrayList<T>();
        }
        ArrayList<T> ret = new ArrayList<T>();
        for (T v : vs) {
            // they are sorted by start location, so if this one starts too late
            // then all of the others will, too.
            if (v.getStart() > query.getEnd()) {
                break;
            }
            if (query.overlaps(v)) {
                ret.add(v);
            }
        }
        return ret;
    }

    private void sortVariants() {
        vs.sort(new Comparator<Locatable>() {
            @Override
            public int compare(Locatable o1, Locatable o2) {
                if (o1.getContig().equals(o2.getContig())) {
                    return Integer.compare(o1.getStart(), o2.getStart());
                } else {
                    return o1.getContig().compareTo(o2.getContig());
                }
            }
        });
    }

    // returns an index into the vs array s.t. no entry before that index
    // reaches (or extends beyond) the given position.
    private int firstPotentiallyReaching(int position) {
        for (int i=0; i<reach.length; i++) {
            if (reach[i]>=position) {
                return i<<shift;
            }
        }
        // no one reaches to the given position.
        return vs.size()-1;
    }

    private static int floorLog2(int n){
        if (n <= 0) {
            throw new IllegalArgumentException();
        }
        // size of int is 32 bits
        return 31 - Integer.numberOfLeadingZeros(n);
    }



}

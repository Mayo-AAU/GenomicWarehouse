package hbase;

/**
 * Created by m102417 on 2/3/16.
 */
public class VCFParserConfig {

    /**
     * configuration variables
     */
    private boolean includeNON_REF = false;

    //todo: put this stuff out to a properties file
    //designation for if a line is a reference call or not.
    public final String NON_REF = "<NON_REF>";

    /**
     * method to determine if we should include a line based on the configuration setup
     * @param line
     * @return
     */
    public boolean includeline(String line){
        //todo: placeholder for all of the loading options that will inevitably follow (e.g. throw out problematic lines ect)
        if(includeNON_REF && line.contains(NON_REF)){  //todo: we may still want to load data from a line with non-ref in it; need to check with steve
            return false;
        }
        return true;
    }
}

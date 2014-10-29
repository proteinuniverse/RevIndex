// Program:  mrRevInd.java
//
// First pass at a MapReduce version of a program which will create
// a reverse-indexed version of a given accumulo table.  The table is
// assumed to be multiple column implemented by using column family as
// the column-name.  One column name is selected to be used for the new
// index, and that will become the row id in the new table. All columns
// will be copied and optionaly the original row id will be placed in 
// a column with a new column name given by the user on the command line.
//
// This was hacked together from Apache Accumulo 1.6 examples RowHash,
// Wordcount, and others and using the documentation here:
// http://accumulo.apache.org/1.6/accumulo_user_manual.html#_mapreduce


package SDFdemo;

import java.io.IOException;
import java.util.*; 

import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.beust.jcommander.Parameter;

public class mrRevInd extends Configured implements Tool 
   {

    // Here we add our own options to the standard Accumulo options

    private static class Opts extends ClientOnRequiredTable 
       {
        @Parameter( names = { "-rt", "--revTablename" }, required = true,
                    description = "name of output reverse indexed table" )
        String revTablename = null;

        @Parameter( names = { "-rf", "--revOnColumnFamily" }, required = true,
                    description = "column family name to use as new row"
                   )
        String revOnColumnFamily = null;

        @Parameter( names = { "-bf", "--backLinkColumnFamily" }, required = true,
                    description = "column family for the back link to old row id"
                   )
        String backLinkColumnFamily = null;

        @Parameter( names = { "-nr", "--numReduceTasks" }, 
                    description = "number of Reducer Tasks (default 1)"
                   )
        String numReduceTasks = null;

        @Parameter( names = "--cull", 
                    description = "comma-separated list of column family to not include in output table")
        String cullString = null;
       }

    // The Mapper class that given an Accumlo table Key-Value pair as input, will
    // extract the row and pack the column family, qualifier, and value together as
    // Text strings to be passed to the Reducer.  Multiple column-family/qual and value
    // strings will be collected for the same row ids, and fed to the reducer as a list,
    // so the reducer will handling everything for a row id all in one shot.
   
    public static class RevMapper extends Mapper<Key,Value,Text,Text> 
       {
        @Override
        public void map(Key k, Value val, Context context) 
          throws IOException, InterruptedException
         {
          // we want to send the "row" part of the Accumulo key "k" as 
          // as the output key to the reducer, so that the reducer gets 
          // all the records corresponding to "row" in one fell swoop
          //
          context.write( k.getRow(), 
                         packv( k.getColumnFamily(), k.getColumnQualifier(), val ) );
          context.progress();  // ? what does this do?  track % progress?
         }

        
        @Override
        public void setup( Context context )   // placeholder in case we need to do
           {                                   // some mapper initializiation
           }

        // not sure the best way to combine the column family and qualifier and
        // accumulo value fields for transmission to the reducer.   
        // for now, lets just concatenate them with ":" separators.
        // This probably will slow down parsing in reducer, so  
        // eventually we want a better way to make a writeable value
        // (custom Writeable based on a structure, maybe?)

        // TODO - 1) make the storage a static 
        //        2) come up with a better way to pack
        Text packv( Text fam, Text qual, Value val )
           {
            return( new Text( fam.toString() + ":" + qual.toString() + ":" + val.toString() ) );
           }
       }


    //  The reducer class should receive all the accumulo keys with the same row in 
    //  one shot.  Row arrives as the key input, and the Value list is a list
    //  of family, qualifier and values, packed together.
    
    public static class RevReducer extends Reducer<Text,Text,Text,Mutation>
       {
        static String revOnColumnFamily;        // setup() loads these from the 
        static String backLinkColumnFamily;     // configuration object
        static String new_row_id = null;

        @Override
        public void setup( Context context )     // get info from configuration (set by run())
           {                                     // and place in class variables
            Configuration conf = context.getConfiguration();
            revOnColumnFamily = conf.get( "revOnColumnFamily" );
            backLinkColumnFamily = conf.get( "backLinkColumnFamily" );
           }

        @Override
        public void reduce( Text old_row_id, Iterable<Text> tvalues, Context c )
          throws IOException, InterruptedException
           {
            ArrayList<String> ts = new ArrayList<String>();

            // okay - we have the old_row id. Now lets scan through the tvalues looking
            // for the family and qualifier that will become the new row_id in the new
            // table.   This is somewhat ugly having to scan through the list twice,
            // but I'm not sure how else to go about identifying the desired family column. 
 
            // TODO don't keep re-allocating Mutation- can we just set it once 
            // for a row id?

            for ( Text tval : tvalues )
               {
                String t = tval.toString();
                String[] parts = unpackv( t ); // from mapper: 0= family, 1= qualifier, 2= value
                if ( revOnColumnFamily.equals( parts[0] ) )
                   {
                    new_row_id = parts[1];     // save this qualifier as the new row id
                    Mutation m = new Mutation( new_row_id );  // lets write the reversed 
                    m.put( new Text( backLinkColumnFamily ),  // record here
                           new Text( old_row_id ),
                           new Value( parts[2].getBytes() ) );
                    c.write( null, m );   // no table name specfied here, so defaultTable is used
                    c.progress();
                   }
                else
                   ts.add( t );
               }

            // now, lets go through the list of values from mapper again
            // and write them out with the new row id replacing the old row id,
            // otherwise unchanged.

            if ( new_row_id != null )
               {
                for ( String t2 : ts )
                   {
                    String[] parts = unpackv( t2 );  // from mapper: 0= family, 1= qualifier, 2= value
                    if ( ! revOnColumnFamily.equals( parts[0] ) ) // skip the main reversed record 
                       {                                          // (we handled it above)
                        Mutation m2 = new Mutation( new_row_id );  // can we just do this once?
                        m2.put( new Text( parts[0] ),
                                new Text( parts[1] ),
                                new Value( parts[2].getBytes() ) );
                        c.write( null, m2 );   // no table name specfied here, so defaultTable is used
                        c.progress();
                       }
                   }
               }
           }

        // TODO - 1) make the storage a static 
        //        2) come up with a better way to pack
        String[] unpackv( String t )
           {
            return( t.split(":") );
           }

      }

    // run() does all the setup and configuration.  This includes parsing the
    // command line arguments and then either using them directly, or passing them
    // to the reducer via the Configuration.

    @Override
    public int run(String[] args) 
        throws Exception 
       {
        int numReduceTasks = 1;

        Opts opts = new Opts();
        opts.parseArgs( mrRevInd.class.getName(), args );

	// pass command line values from --revOnColumnFamily and --backLinkOnColumnFamily
        // to reducer using configuration object.  (These are read and loaded into 
        // static class variables RevReducer.setup() )

        Configuration conf = getConf();
        conf.set( "revOnColumnFamily", opts.revOnColumnFamily );
        conf.set( "backLinkColumnFamily", opts.backLinkColumnFamily );

        Job job = JobUtil.getJob( conf );
        //Job job = JobUtil.getJob( getConf() );
        job.setJobName( this.getClass().getName() );
        job.setJarByClass( this.getClass() );
        if ( opts.numReduceTasks != null )
            numReduceTasks = Integer.parseInt( opts.numReduceTasks );
        
        // this didn't work:
        //String revTablename = opts.revTablename;
        //AccumuloInputFormat.setInputTableName( job, revTablename );

        job.setInputFormatClass( AccumuloInputFormat.class );
        opts.setAccumuloConfigs( job );

        job.setMapperClass( RevMapper.class );
        job.setReducerClass( RevReducer.class );
        
        job.setMapOutputKeyClass( Text.class );
        job.setMapOutputValueClass( Text.class );

        job.setNumReduceTasks( numReduceTasks );

        job.setOutputFormatClass( AccumuloOutputFormat.class );
        boolean createTables = true;
        String defaultTable = opts.revTablename;

        // This is wrong (from the manual)
        //AccumuloOutputFormat.setOutputInfo( job, "root", "secret".getBytes(),
        //                                    createTables, defaultTable );

        // Tried, this, but it said this was set already
        //AccumuloOutputFormat.setConnectorInfo( job, "root", 
        //                                       new PasswordToken( "secret".getBytes() )
        //                                    );

        AccumuloOutputFormat.setCreateTables( job, createTables );
        AccumuloOutputFormat.setDefaultTableName( job, defaultTable );

        // javadoc 1.6 says this is deprecated, should use ClientConfiguration
        // object instead
        //  (said this was set already)
        //AccumuloOutputFormat.setZooKeeperInstance(job, "accumulo", "accum2,accum3" );
                                                                      
        job.waitForCompletion( true );
        return job.isSuccessful() ? 0 : 1;
       }


    public static void main(String[] args) 
        throws Exception 
       {
        ToolRunner.run(new Configuration(), new mrRevInd(), args);
       }
   }


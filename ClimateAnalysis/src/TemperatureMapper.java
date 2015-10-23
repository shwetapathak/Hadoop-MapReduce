import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
public class TemperatureMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable>
{

    public static final int MISSING = -9999;
    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
            throws IOException
    {
        String line = value.toString();
        String [] path = line.split(",");
        
       //int month = Integer.parseInt(path[1]);
        //String station = String.valueOf(Integer.parseInt(path[0]));
        String year = String.valueOf(Integer.parseInt(path[0]));
        
        /*String station_number = "Station";
        station_number = station_number.concat(station);
        String year_no = "Year";
        year_no = station_number.concat(year_no);
        year_no = year_no.concat(year); */
        /*switch(month)
        {
        	case 1: year = year.concat("Jan");
        			break;
        	case 2: year = year.concat("Feb");
        			break;
        	case 3: year = year.concat("Mar");
        			break;
        	case 4: year = year.concat("April");
        			break;
        	case 5: year = year.concat("May");
        			break;
        	case 6: year = year.concat("Jun");
        			break;
        	case 7: year = year.concat("Jul");
        			break;
        	case 8: year = year.concat("Aug");
        			break;
        	case 9: year = year.concat("Sep");
        			break;
        	case 10: year = year.concat("Oct");
        			break;
        	case 11: year = year.concat("Nov");
        			break;
        	case 12: year = year.concat("Dec");
        			break;
        	default: break;
     }*/
        
        /*if(month <= 6)
        {
        	year = year.concat("Summer");
        }
        else
        	year = year.concat("Winter");
      
        */
             
        Double temperature;

        Double Tmaxtemp = Double.parseDouble(path[4].toString());
        Double Tmintemp = Double.parseDouble(path[5].toString());

        temperature = (Tmaxtemp+Tmintemp)/2;
     
        if(Tmaxtemp != MISSING && Tmintemp != MISSING) {
            output.collect(new Text(year), new DoubleWritable(temperature));
        }


    }
}

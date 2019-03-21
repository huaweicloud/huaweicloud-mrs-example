package com.huawei.bigdata.spark.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import com.huawei.hadoop.security.LoginUtil;
import scala.Tuple2;
import scala.Tuple3;
import java.io.File;

public class FemaleInfoCollection
{
    public static void main(String[] args) throws Exception {
        Configuration hadoopConf = new Configuration();
        if("kerberos".equalsIgnoreCase(hadoopConf.get("hadoop.security.authentication"))){
            //security mode

            final String userPrincipal = "sparkuser";
            final String USER_KEYTAB_FILE = "user.keytab";
            String filePath = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
            String krbFile = filePath + "krb5.conf";
            String userKeyTableFile = filePath + USER_KEYTAB_FILE;

            LoginUtil.login(userPrincipal, userKeyTableFile, krbFile, hadoopConf);
        }

		//Create a configuration class SparkConf, and then create a SparkContext.
        SparkConf conf = new SparkConf().setAppName("CollectFemaleInfo");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        // Read the source file data, and transfer each row of records to an element of the RDD.
        JavaRDD<String> data = jsc.textFile(args[0]);

		// Split each column of each record, and generate a Tuple.
        JavaRDD<Tuple3<String,String,Integer>> person = data.map(new Function<String,Tuple3<String,String,Integer>>() {

            private static final long serialVersionUID = -2381522520231963249L;

            @Override
            public Tuple3<String, String, Integer> call(String s) throws Exception
            {
                // Split a row of data by commas (,).
                String[] tokens = s.split(",");
				// Integrate the three split elements to a ternary Tuple.
                Tuple3<String, String, Integer> person = new Tuple3<String, String, Integer>(tokens[0], tokens[1], Integer.parseInt(tokens[2]));

                return person;

            }
        });

		// Use the filter function to filter the data information about the time that female netizens spend online.
        JavaRDD<Tuple3<String,String,Integer>> female = person.filter(new Function<Tuple3<String,String,Integer>, Boolean>() {

            private static final long serialVersionUID = -4210609503909770492L;

            @Override
            public Boolean call(Tuple3<String, String, Integer> person) throws Exception
            {
                //  Filter the records of which the sex in the second column is female.
				Boolean isFemale = person._2().equals("female");

                return isFemale;
            }
        });

		// Aggregate the total time that each female netizen spends online.
        JavaPairRDD<String, Integer> females = female.mapToPair(new PairFunction<Tuple3<String, String, Integer>, String, Integer>()
        {
            private static final long serialVersionUID = 8313245377656164868L;

            @Override
            public Tuple2<String, Integer> call(Tuple3<String, String, Integer> female) throws Exception
            {
                // Extract the two columns representing the name and online time for the sum of online time by name during further operations.
				Tuple2<String, Integer> femaleAndTime = new  Tuple2<String, Integer>(female._1(), female._3());

                return femaleAndTime;
            }
        });

        JavaPairRDD<String, Integer> femaleTime = females.reduceByKey(new Function2<Integer, Integer, Integer>()
        {
            private static final long serialVersionUID = -3271456048413349559L;

            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception
            {
                // Sum two online time durations of the same female netizen.
				return (integer + integer2);
            }
        });

        // Filter the information about female netizens who spend more than 2 hours online.
		JavaPairRDD<String, Integer> rightFemales = femaleTime.filter(new Function<Tuple2<String, Integer>, Boolean>()
        {
            private static final long serialVersionUID = -3178168214712105171L;

            @Override
            public Boolean call(Tuple2<String, Integer> s) throws Exception
            {
                // Extract the total time that female netizens spend online, and determine whether the time is more than 2 hours.
				if(s._2() > (2 * 60)) {
                    return true;
                }
                return false;
            }
        });

        // Print the information about female netizens who meet the requirements.
		for(Tuple2<String, Integer> d: rightFemales.collect()) {
            System.out.println(d._1() + "," + d._2());
        }

	jsc.stop();
    }
}


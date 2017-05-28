/**
 * Created by caitlin.ye on 5/20/17.
 */
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class DBOutputWritable implements DBWritable{

    private String starting_phrase;
    private String following_word;
    private int count;

    //see notes to understand why structured this way (lang model)
    //easy to use, as many columns you want, this has 3 col
    //Encapsulation
    //write to MySQL db
    public DBOutputWritable(String starting_phrase, String following_word, int count) {
        this.starting_phrase = starting_phrase;
        this.following_word = following_word;
        this.count= count;
    }



    //below functions for reading writing to db
    public void readFields(ResultSet arg0) throws SQLException {
        this.starting_phrase = arg0.getString(1);  //get it from code block,   insert into table where values = blah blah,
        //MR db communicate, queries db
        this.following_word = arg0.getString(2);
        this.count = arg0.getInt(3);

    }

    public void write(PreparedStatement arg0) throws SQLException {
        arg0.setString(1, starting_phrase);  //execute query statement
        arg0.setString(2, following_word);
        arg0.setInt(3, count);

    }

}


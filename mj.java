import java.sql.*;
import java.time.Month;
import java.lang.Thread;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Scanner;
import java.util.HashMap;

public class mj
{
    static String db_name = "db";
    static String db_usrnme = "root";
    static String pass = "";

    static String dw_name = "dw";
    static String dw_usrnme = "root";
    static String passwordd = "";

    public static class stream extends Thread{

        public class Datasource {
            Double t_id;
            String p_id;
            String c_id;
            String ste_id;
            String ste_name;
            String tme_id;
            Date t_dte;
            Double qty;

            public Datasource (Double transaction_id,String product_id, String customer_id, String store_id, String store_name, String time_id,Date t_date,Double quantity )
            {
                this.t_id = transaction_id;
                this.p_id = product_id;
                this.c_id = customer_id;
                this.ste_id = store_id;
                this.ste_name = store_name;
                this.tme_id = time_id;
                this.t_dte = t_date;
                this.qty = quantity;
            }
        }
        
        static Queue<Datasource> stream_buffer = new LinkedList<>();

        @Override
        public void run()
        {
            try
            {
                String db_url = "jdbc:mysql://127.0.0.1:3306/"+db_name;

                Connection c = DriverManager.getConnection(db_url,
                db_usrnme, pass);
                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("SELECT * FROM db.transactions");
                
                while (!interrupted() && rs.next()) 
                {
                    Double transaction_id = rs.getDouble("TRANSACTION_ID");
                    String product_id = rs.getString("PRODUCT_ID");
                    String customer_id = rs.getString("CUSTOMER_ID");
                    String store_id = rs.getString("STORE_ID");
                    String store_name = rs.getString("STORE_NAME");
                    String time_id = rs.getString("TIME_ID");
                    Date t_date = rs.getDate("T_DATE");
                    Double quantity = rs.getDouble("QUANTITY");

                    Datasource ds = new Datasource(transaction_id, product_id, customer_id, store_id, store_name, time_id, t_date, quantity);

                    stream_buffer.add(ds);
                }
            } 
            catch (SQLException se) {
            }
        }
    }

    public static class masterData extends Thread
    {
        public class Product {
            String p_id;
            String p_name;
            String sup_id;
            String sup_name;
            Double price;
            
            public Product(String product_id,String product_name,String supplier_id, String supplier_name,Double product_price)
            {
                this.p_id = product_id;
                this.p_name = product_name;
                this.sup_id = supplier_id;
                this.sup_name = supplier_name;
                this.price = product_price;
            }
        }

        public class Customer
        {
            String cust_id;
            String cust_name;

            public Customer(String customer_id,String customer_name)
            {
                this.cust_id = customer_id;
                this.cust_name = customer_name;
            }
        }

        static Queue<Customer> md_buffer_cus = new LinkedList<>();
        static Queue<Product> md_buffer_pdt = new LinkedList<>();

        @Override
        public void run()
        {
            try
            {
                String db_url = "jdbc:mysql://127.0.0.1:3306/"+db_name;

                Connection c = DriverManager.getConnection(db_url,
                db_usrnme, pass);

                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("SELECT * FROM db.customers");
                
                while (!interrupted() && rs.next()) {
    
                    String customer_id = rs.getString("CUSTOMER_ID");
                    String customer_name = rs.getString("CUSTOMER_NAME");

                    Customer cus = new Customer(customer_id,customer_name);
                    md_buffer_cus.add(cus);
                }

                ResultSet rs2 = s.executeQuery("SELECT * FROM db.products");

                while (!interrupted() && rs2.next()) {
    
                    String product_id = rs2.getString("PRODUCT_ID");
                    String product_name = rs2.getString("PRODUCT_NAME");
                    String supplier_id = rs2.getString("SUPPLIER_ID");
                    String supplier_name = rs2.getString("SUPPLIER_NAME");
                    Double product_price = rs2.getDouble("PRICE");
                    
                    Product prd = new Product(product_id,product_name,supplier_id,supplier_name,product_price);
                    md_buffer_pdt.add(prd);
                }

            } catch (SQLException se) {
            }
        }
    }

    static public class transdata
    {
        Double transaction_id;
        String product_id;
        String customer_id;
        String time_id;
        String store_id;
        String store_name;
        Date t_date;
        Double quantity;
        String product_name;
        String supplier_id;
        String supplier_name;
        Double price;
        Double sale;
        String customer_name;

        public transdata(mj.stream.Datasource ds_obj,String prod_name,String sup_id,String sup_name,Double prc,String cus_name)
        {
            this.transaction_id = ds_obj.t_id;
            this.product_id = ds_obj.p_id;
            this.customer_id = ds_obj.c_id;
            this.time_id = ds_obj.tme_id;
            this.store_id = ds_obj.ste_id;
            this.store_name = ds_obj.ste_name;
            this.t_date = ds_obj.t_dte;
            this.quantity = ds_obj.qty;
            this.product_name = prod_name;
            this.supplier_id = sup_id;
            this.supplier_name = sup_name;
            this.price = prc;
            this.sale = this.quantity * this.price;
            this.customer_name = cus_name;
        }
    }

    static void readfromMD(mj.stream.Datasource ds,mj.transdata tsd) //meshjoin
    {
        int foundcus = 0, foundprd = 0;

        while (foundcus != 1)
        { 
            mj.masterData.Customer rmhead = mj.masterData.md_buffer_cus.remove();
            
            if (rmhead.cust_id.compareToIgnoreCase(ds.c_id) == 0)              //Customer Join
            {
                tsd.customer_name = rmhead.cust_name;
                foundcus = 1;    
            }
            mj.masterData.md_buffer_cus.add(rmhead);
        }

        while (foundprd != 1)
        { 
            mj.masterData.Product rmhead = mj.masterData.md_buffer_pdt.remove();

            // String buffer_p_id = rmhead.p_id;
            // String stream_p_id = ds.p_id;

            if (rmhead.p_id.compareToIgnoreCase(ds.p_id) == 0)              //Product Join
            {
                tsd.product_name = rmhead.p_name;
                tsd.supplier_id = rmhead.sup_id;
                tsd.supplier_name = rmhead.sup_name;
                tsd.price = rmhead.price;
                tsd.sale = tsd.quantity * tsd.price;
                foundprd = 1;    
            }
            mj.masterData.md_buffer_pdt.add(rmhead);
        }
    }

    static HashMap<Double, transdata> Hashtable = new HashMap<Double, transdata>();

    public static void main(String[] args) {

        Scanner sc= new Scanner(System.in);  
        System.out.print("Enter database name: ");  
        db_name = sc.nextLine();  

        System.out.print("Enter db username: ");  
        db_usrnme= sc.nextLine();                

        System.out.print("Enter db password: ");   
            
        if (sc.hasNextLine())
        {
            pass = sc.nextLine();
        }
        sc.close();
        
        Thread md = new Thread(new masterData()); //Starts loading MD
        md.run();                          

        Thread s = new Thread(new stream()); //Starts stream using transaction data
        s.run();                             

        while (!mj.stream.stream_buffer.isEmpty())
        {
            mj.stream.Datasource ext_ds = mj.stream.stream_buffer.remove();   //Extract Head of Queue

            mj.transdata tmd = new mj.transdata(ext_ds, "", "", "", 0.0, ""); //Tuple of Transformed Data

            readfromMD(ext_ds,tmd); //Apply Join on product and customer id

            mj.Hashtable.put(tmd.transaction_id, tmd); //Add Final tuple in Hashtable
        }

        //Populate DW

        Connection conn = null;
        try {

            String dw_url = "jdbc:mysql://127.0.0.1:3306/"+dw_name;

            conn = DriverManager.getConnection(dw_url,
            dw_usrnme, passwordd);

        } catch (SQLException e) {
            e.printStackTrace();
        }

        for (Double i : mj.Hashtable.keySet()) {

            try {
                Statement stmt = conn.createStatement();

                try {   //Product Table

                    String p_id = new String("'"+mj.Hashtable.get(i).product_id+"'");
                    String p_name = new String("'"+mj.Hashtable.get(i).product_name+"'");
                    Double p_prc = mj.Hashtable.get(i).price;

                    String sqlInsert = "insert into product values ("+p_id+","+p_name+","+p_prc+")";    
                    stmt.executeUpdate(sqlInsert);

                } catch (java.sql.SQLIntegrityConstraintViolationException e) {}//Filter out duplicate entries

                try {   //Supplier Table

                    String s_id = new String("'"+mj.Hashtable.get(i).supplier_id+"'");
                    String s_name = new String("'"+mj.Hashtable.get(i).supplier_name+"'");
                
                    String sqlInsert = "insert into supplier values ("+s_id+","+s_name+")";    
                    stmt.executeUpdate(sqlInsert);

                } catch (java.sql.SQLIntegrityConstraintViolationException e) {}//Filter out duplicate entries

                try {   //Customer Table

                    String c_id = new String("'"+mj.Hashtable.get(i).customer_id+"'");
                    String c_name = new String("'"+mj.Hashtable.get(i).customer_name+"'");
                
                    String sqlInsert = "insert into customer values ("+c_id+","+c_name+")";    
                    stmt.executeUpdate(sqlInsert);

                } catch (java.sql.SQLIntegrityConstraintViolationException e) {}//Filter out duplicate entries
            
                try {   //Store Table

                    String str_id = new String("'"+mj.Hashtable.get(i).store_id+"'");
                    String str_name = new String("'"+mj.Hashtable.get(i).store_name+"'");
                
                    String sqlInsert = "insert into store values ("+str_id+","+str_name+")";    
                    stmt.executeUpdate(sqlInsert);

                } catch (java.sql.SQLIntegrityConstraintViolationException e) {}//Filter out duplicate entries

                try {   //Date Table

                    String tme_id = new String("'"+mj.Hashtable.get(i).time_id+"'");
                    String t_dte = "'"+mj.Hashtable.get(i).t_date.toString()+"'";
                    String t_month = "'"+Month.of((mj.Hashtable.get(i).t_date.getMonth()+1)).name().toLowerCase()+"'";
                    int t_year = mj.Hashtable.get(i).t_date.getYear()+1900;
                    int t_weekend = 0;
                    String t_quarter = "";
                    String t_half = "";
                    
                    if ( mj.Hashtable.get(i).t_date.getMonth() <= 2 )  //Setting Quarter value
                    {
                        t_quarter = "'Q1'";
                        t_half = "'First'";
                    }
                    else if (mj.Hashtable.get(i).t_date.getMonth() >= 3 && mj.Hashtable.get(i).t_date.getMonth() <= 5) {
                        t_quarter = "'Q2'";
                        t_half = "'First'";
                    }
                    else if (mj.Hashtable.get(i).t_date.getMonth() >= 6 && mj.Hashtable.get(i).t_date.getMonth() <= 8) {
                        t_quarter = "'Q3'";
                        t_half = "'Second'";
                    }
                    else if (mj.Hashtable.get(i).t_date.getMonth() >= 9 && mj.Hashtable.get(i).t_date.getMonth() <= 11) {
                        t_quarter = "'Q4'";
                        t_half = "'Second'";
                    }

                    if ( mj.Hashtable.get(i).t_date.getDay() == 0 || mj.Hashtable.get(i).t_date.getDay() == 6) 
                    {
                        t_weekend = 1;
                    } else 
                    {
                        t_weekend = 0;
                    }

                    String sqlInsert = "insert into date values ("+tme_id+","+t_dte+","+t_weekend+","+t_half+","+t_month+","+t_quarter+","+t_year+")";    
                    stmt.executeUpdate(sqlInsert);

                } catch (java.sql.SQLIntegrityConstraintViolationException e) {}//Filter out duplicate entries

                try {   //Sales Table

                    Double transaction_id = i;
                    String prd_id = "'"+mj.Hashtable.get(i).product_id+"'";
                    String cus_id = "'"+mj.Hashtable.get(i).customer_id+"'";
                    String tme_id = "'"+mj.Hashtable.get(i).time_id+"'";
                    String str_id = "'"+mj.Hashtable.get(i).store_id+"'";
                    String sup_id = "'"+mj.Hashtable.get(i).supplier_id+"'";
                    Double qty = mj.Hashtable.get(i).quantity;
                    Double sle = mj.Hashtable.get(i).sale;

                    String sqlInsert = "insert into sales values ("+transaction_id+","+prd_id+","+cus_id+","+tme_id+","+str_id+","+sup_id+","+qty+","+sle+")";    
                    stmt.executeUpdate(sqlInsert);

                } catch (java.sql.SQLIntegrityConstraintViolationException e) {}//Filter out duplicate entries
            }
            catch (SQLException e) {

                e.printStackTrace();
            }
        }
    }
}
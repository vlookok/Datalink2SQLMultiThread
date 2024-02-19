/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 * 
 * Testing New Comment
 */
package com.mycompany.datalink2sqlmultithread;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
//import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Properties;
import java.util.logging.Level;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.input.ReversedLinesFileReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author vlook
 */

class DataPopMThread {
/**
 * @param args the command line arguments
*/
    public static final Logger logger = LogManager.getLogger("DataPopMThread");

    
    static class DPrunnable implements Runnable {
        private String passdir;
        private String passcountry;    
        private String passexch;
        private String appJDBCcon;
        private String appDBUname;
        private String appDBPassw;

        private Connection con;
                
        DPrunnable(String passdir, String passcountry, String passexch, String passcon, String passname, String passpassw){
            this.passdir=passdir;
            this.passcountry=passcountry;
            this.passexch=passexch;
            this.appJDBCcon=passcon;
            this.appDBUname=passname;
            this.appDBPassw=passpassw;
        }
    
        @Override
        public void run() {
            try {
                String[] data;
                String symbol;
                String symName;
                String fdate;
                String fopen;
                String fhigh;
                String fllow;
                String fclose;
                String fvolume;
                
                //String sqlst1;
                ResultSet rs1;
                ResultSet rs3;
                //String sqlst2;
                //int rs2;
                String filename;
                int sqlbatch1=0;
                int sqlbatch2=0;
                int j;
                            
  //          PreparedStatement ps = con.prepareStatement("INSERT INTO account(accountnumber, accountname, accounttype,broker) VALUES(?,?,'1234','abc')");
  //          ps.setString(1, "23456789");
  //          ps.setString(2, "testtest");
  //          ps.execute();
                con = DriverManager.getConnection(appJDBCcon,appDBUname,appDBPassw);  
                PreparedStatement ps1 = con.prepareStatement("select Symbol, Datetrx from nasdaqstprice where symbol = ? AND Datetrx = ?");
                ps1.setFetchSize(5);
                PreparedStatement ps2 = con.prepareStatement("Insert into nasdaqstprice (Symbol, DateTrx, `Open`, High, Low, `Close`, Volume) VALUES(?, ?, ?, ?, ?, ?, ?)"); 
                PreparedStatement ps5 = con.prepareStatement("Update nasdaqstprice SET `Open`=?, High=?, Low=?, `Close`=?, Volume=? where Symbol=? and DateTrx=?"); 
                
                if ("TSV".equals(passexch)) {
                    ps1 = con.prepareStatement("select Symbol, Datetrx from tsvprice where symbol = ? AND Datetrx = ?");
                    ps1.setFetchSize(5);
                    ps2 = con.prepareStatement("Insert into tsvprice (Symbol, DateTrx, `Open`, High, Low, `Close`, Volume) VALUES(?, ?, ?, ?, ?, ?, ?)");
                    ps5 = con.prepareStatement("Update tsvprice SET `Open`=?, High=?, Low=?, `Close`=?, Volume=? where Symbol=? and DateTrx=?");                     
                }else if ("NYSE".equals(passexch)){
                    ps1 = con.prepareStatement("select Symbol, Datetrx from nysestprice where symbol = ? AND Datetrx = ?");
                    ps1.setFetchSize(5);
                    ps2 = con.prepareStatement("Insert into nysestprice (Symbol, DateTrx, `Open`, High, Low, `Close`, Volume) VALUES(?, ?, ?, ?, ?, ?, ?)");
                    ps5 = con.prepareStatement("Update nysestprice SET `Open`=?, High=?, Low=?, `Close`=?, Volume=? where Symbol=? and DateTrx=?");
                }else if ("AMEX".equals(passexch)){
                    ps1 = con.prepareStatement("select Symbol, Datetrx from amexprice where symbol = ? AND Datetrx = ?");
                    ps1.setFetchSize(5);
                    ps2 = con.prepareStatement("Insert into amexprice (Symbol, DateTrx, `Open`, High, Low, `Close`, Volume) VALUES(?, ?, ?, ?, ?, ?, ?)");
                    ps5 = con.prepareStatement("Update amexprice SET `Open`=?, High=?, Low=?, `Close`=?, Volume=? where Symbol=? and DateTrx=?");
                }else if ("TSX".equals(passexch)){
                    ps1 = con.prepareStatement("select Symbol, Datetrx from tsxprice where symbol = ? AND Datetrx = ?");
                    ps1.setFetchSize(5);
                    ps2 = con.prepareStatement("Insert into tsxprice (Symbol, DateTrx, `Open`, High, Low, `Close`, Volume) VALUES(?, ?, ?, ?, ?, ?, ?)");
                    ps5 = con.prepareStatement("Update tsxprice SET `Open`=?, High=?, Low=?, `Close`=?, Volume=? where Symbol=? and DateTrx=?");
                }
                PreparedStatement ps3 = con.prepareStatement("select Symbol, Exchange from listofStocks where symbol = ? AND Country = ?");
                ps3.setFetchSize(5);
                PreparedStatement ps4 = con.prepareStatement("INSERT INTO listofStocks (Symbol, Name, Exchange, Type, Country) VALUES(?, ?, ?, 'STOCK', ?)");
                //Statement stmt1=con.createStatement();
                //Statement stmt2=con.createStatement();
                SimpleDateFormat sdfin = new java.text.SimpleDateFormat("MM/dd/yyyy HH:mm:ss a");
                SimpleDateFormat sdfout = new java.text.SimpleDateFormat("yyyy-MM-dd");
                
                @SuppressWarnings("rawtypes")
                Iterator it = FileUtils.iterateFiles(new File(passdir), null, false);
                while(it.hasNext()){
                    filename = passdir+"\\"+((File) it.next()).getName();
                    logger.info(filename);
                    @SuppressWarnings("resource")
                    ReversedLinesFileReader stockfile = new ReversedLinesFileReader(new File(filename),Charset.forName("UTF-8"));
                    for(j=0;j<10000;j++){
                        String line=stockfile.readLine();
                        if(line==null)
                            break;
                        //data=line.split(",");
                        data=line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
                        logger.info(line);
//                    System.out.println(line);
                        if ("INTERVAL".equals(data[1]))
                            break;
                        symbol = data[0];
                        symName = data[2].replace("'","\\\'");
                        fdate = sdfout.format(sdfin.parse(data[3]));
                        fopen = data[4];
                        fhigh = data[5];
                        fllow = data[6];
                        fclose = data[7];
                        fvolume = data.length == 9 ? data[8]: "0";
                        if (j==0) {
                            ps3.setString(1, symbol);
                            ps3.setString(2, passcountry);
                            rs1=ps3.executeQuery();
                            //sqlst1="select * from listofStocks where symbol = '" + symbol + "' AND Country = '" + passcountry + "';";
                            //rs1=stmt1.executeQuery(sqlst1);
                            //if (rs1.next()==false) {
                            if (rs1.next()==false) {
                                logger.info("#######NEW_ListofStock##" + j + "###" + line);
                                ps4.setString(1, symbol);
                                ps4.setString(2, symName);
                                ps4.setString(3, passexch);
                                ps4.setString(4, passcountry);
                                ps4.executeUpdate();
                                //sqlst1="INSERT INTO listofStocks (Symbol, Name, Exchange, Type, Country) VALUES ('"
                                //    + symbol +"', '"+symName+"', '" + passexch + "', 'STOCK', '"+passcountry+"');";
                                //rs2=stmt1.executeUpdate(sqlst1);
                            }
                            //sqlst2="select * from Stockprice where symbol = '" + symbol + "' AND Datetrx = '" + fdate + "';";	
                            //rs1=stmt2.executeQuery(sqlst2);
                            //if (rs1.next()==false) {
                            //    sqlst2="Insert into Stockprice (Symbol, DateTrx, Open, High, Low, Close, Volume) VALUES ('"
                            //        + symbol + "', '" + fdate + "', '" + fopen + "', '" + fhigh + "', '" + fllow + "', '"
                            //        + fclose + "', '" + fvolume + "');";
                            //    rs2=stmt2.executeUpdate(sqlst2);
                        }
                        ps1.setString(1, symbol);
                        ps1.setString(2, fdate);
                        rs3=ps1.executeQuery();
                        //if (rs3.next()==true) logger.info("#######FOUND1#####" + symbol + "###" + fdate+"###" +j);
                        //if (rs3.next()==true) logger.info("#######FOUND2#####" + symbol + "###" + fdate);
                        //sqlst1="select * from nasdaqstprice where symbol = '" + symbol + "' AND Datetrx = '" + fdate + "';";
                        //rs1=stmt2.executeQuery(sqlst1);
                        if (rs3.next()==false) {
                            ps2.setString(1, symbol);
                            ps2.setString(2, fdate);
                            ps2.setFloat(3, Float.parseFloat(fopen));
                            ps2.setFloat(4, Float.parseFloat(fhigh));                                
                            ps2.setFloat(5, Float.parseFloat(fllow));                                
                            ps2.setFloat(6, Float.parseFloat(fclose));
                            ps2.setLong(7, /*Long.parseLong(fvolume)*/Math.round(Float.parseFloat(fvolume)));
//                            ps2.executeUpdate();
                            ps2.addBatch();
                            if (sqlbatch1%50==0) { ps2.executeBatch();}
                            sqlbatch1++;
                            //sqlst1="Insert into nasdaqstprice (Symbol, DateTrx, Open, High, Low, Close, Volume) VALUES ('"
                            //    + symbol + "', '" + fdate + "', '" + fopen + "', '" + fhigh + "', '" + fllow + "', '"
                            //    + fclose + "', '" + fvolume + "');";
                            //rs2=stmt2.executeUpdate(sqlst1);
                        }else {
                            if (j<=200 /*4 && (rs3.next()==true)*/) {
                                ps5.setFloat(1, Float.parseFloat(fopen));
                                ps5.setFloat(2, Float.parseFloat(fhigh));                                
                                ps5.setFloat(3, Float.parseFloat(fllow));                                
                                ps5.setFloat(4, Float.parseFloat(fclose));
                                ps5.setLong(5, /*Long.parseLong(fvolume)*/Math.round(Float.parseFloat(fvolume)));
                                ps5.setString(6, symbol);
                                ps5.setString(7, fdate);
//                            ps2.executeUpdate();
                                ps5.addBatch();
                                if (sqlbatch2%50==0) { ps5.executeBatch();}
                                    sqlbatch2++;
                            } else break;
                        }
                        /*if ("NASDAQ".equals(passexch)){
                            sqlst1="select * from nasdaqstprice where symbol = '" + symbol + "' AND Datetrx = '" + fdate + "';";
                            rs1=stmt2.executeQuery(sqlst1);
                            if (rs1.next() == false) {
                                sqlst1="Insert into nasdaqstprice (Symbol, DateTrx, Open, High, Low, Close, Volume) VALUES ('"
                                    + symbol + "', '" + fdate + "', '" + fopen + "', '" + fhigh + "', '" + fllow + "', '"
                                    + fclose + "', '" + fvolume + "');";
                                rs2=stmt2.executeUpdate(sqlst1);
                            }else break;
                        }else if ("NYSE".equals(passexch)){
                            sqlst2="select * from nysestprice where symbol = '" + symbol + "' AND Datetrx = '" + fdate + "';";
                            rs1=stmt2.executeQuery(sqlst2);
                            if (rs1.next() == false) {
                                sqlst2="Insert into nysestprice (Symbol, DateTrx, Open, High, Low, Close, Volume) VALUES ('"
                                        + symbol + "', '" + fdate + "', '" + fopen + "', '" + fhigh + "', '" + fllow + "', '"
                                        + fclose + "', '" + fvolume + "');";
                                rs2=stmt2.executeUpdate(sqlst2);
                            }else break;
                        }else if ("AMEX".equals(passexch)){
                            sqlst1="select * from amexprice where symbol = '" + symbol + "' AND Datetrx = '" + fdate + "';";
                            rs1=stmt2.executeQuery(sqlst1);
                            if (rs1.next() == false) {
                                sqlst1="Insert into amexprice (Symbol, DateTrx, Open, High, Low, Close, Volume) VALUES ('"
                                        + symbol + "', '" + fdate + "', '" + fopen + "', '" + fhigh + "', '" + fllow + "', '"
                                        + fclose + "', '" + fvolume + "');";
                                rs2=stmt2.executeUpdate(sqlst1);
                            }else break;
                        }else if ("TSX".equals(passexch)){
                            sqlst2="select * from tsxprice where symbol = '" + symbol + "' AND Datetrx = '" + fdate + "';";
                            rs1=stmt2.executeQuery(sqlst2);
                            if (rs1.next() == false) {
                                sqlst2="Insert into tsxprice (Symbol, DateTrx, Open, High, Low, Close, Volume) VALUES ('"
                                        + symbol + "', '" + fdate + "', '" + fopen + "', '" + fhigh + "', '" + fllow + "', '"
                                        + fclose + "', '" + fvolume + "');";
                                rs2=stmt2.executeUpdate(sqlst2);
                            }else break;
                        }else if ("TSV".equals(passexch)){
                            sqlst1="select * from tsvprice where symbol = '" + symbol + "' AND Datetrx = '" + fdate + "';";
                            rs1=stmt2.executeQuery(sqlst1);
                            if (rs1.next() == false) {
                                sqlst1="Insert into tsvprice (Symbol, DateTrx, Open, High, Low, Close, Volume) VALUES ('"
                                        + symbol + "', '" + fdate + "', '" + fopen + "', '" + fhigh + "', '" + fllow + "', '"
                                        + fclose + "', '" + fvolume + "');";
                                rs2=stmt2.executeUpdate(sqlst1);
                            }else break;
                        }*/
                    }
                    ps2.executeBatch();
                    ps5.executeBatch();
                }
//                ps2.executeBatch();
//                ps5.executeBatch();
                con.close();
            } catch (ParseException | SQLException | IOException ex) {
                java.util.logging.Logger.getLogger(DataPopMThread.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
    public static void main(String[] args) throws ParseException, InterruptedException, SQLException {

        try{  
            //appProps.load(new FileInputStream("PopDir.properties"));
            Properties appProps = new Properties();
            InputStream input = DataPopMThread.class.getClassLoader().getResourceAsStream("PopDir.properties");
            appProps.load(input);
            String appTotal = appProps.getProperty("Total");
            String appJDBCcon = appProps.getProperty("JDBCcon");
            String appDBUname = appProps.getProperty("DBUname");
            String appDBPassw = appProps.getProperty("DBPassw");
            String[][] appDir = new String[Integer.parseInt(appTotal)][2];
            String fcountry="";
                      
            Class.forName("com.mysql.cj.jdbc.Driver");
            
            DPrunnable[] DPrlist = new DPrunnable[Integer.parseInt(appTotal)];
            Thread[] Threadlist = new Thread[Integer.parseInt(appTotal)];
            
            for(int i=0;i<Integer.parseInt(appTotal); i++) {
                appDir[i][0] = appProps.getProperty("Dir"+(i+1));
                appDir[i][1] = appProps.getProperty("Dir"+(i+1)+"dir");
                if ("NYSE".equals(appDir[i][0]) || "NASDAQ".equals(appDir[i][0]) || "AMEX".equals(appDir[i][0])){
                    fcountry = "USA"; 
                } else if ("TSX".equals(appDir[i][0]) || "TSV".equals(appDir[i][0])){
                    fcountry = "CANADA";
                } else if ("SGX".equals(appDir[i][0])){
                    fcountry = "SINGAPORE";
                } else if ("HKEX".equals(appDir[i][0])){
                    fcountry = "HongKong";
                }
                logger.info(appDir[i][1]);
                DPrlist[i] = new DPrunnable(appDir[i][1], fcountry, appDir[i][0], appJDBCcon, appDBUname, appDBPassw);
                Threadlist[i]= new Thread(DPrlist[i]);
                Threadlist[i].start();
            }
            for(int i=0;i<Integer.parseInt(appTotal); i++){
                Threadlist[i].join();
            }
        }catch(IOException | ClassNotFoundException | NumberFormatException  e){ System.out.println(e);}
    }
}
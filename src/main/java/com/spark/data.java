package com.spark;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

/**
 * Created by hadoop on 10/2/17.
 */
public class data {
    private String accountId;
    private double serialNumber;
    private String packageName;
    private String pageName;
    private long startTime;
    private long endTime;


    public String getAccountId()
    {
        return accountId;
    }

    public void setAccountId(String accountId)
    {
        this.accountId = accountId;
    }

    public double getSerialNumber(){ return serialNumber;}

    public void setSerialNumber(double serialNumber){ this.serialNumber = serialNumber; }

    public String getPackageName(){ return packageName; }

    public void setPackageName(String packageName){ this.packageName = packageName; }

    public String getPageName(){ return pageName; }

    public void setPageName(String pageName){ this.pageName = pageName; }

    public long getStartTime(){ return startTime; }

    public void setStartTime(long startTime){ this.startTime = startTime; }

    public long getEndTime(){ return endTime; }

    public void setEndTime(long endTime){ this.endTime = endTime; }

    public Row toRow()
    {
        return RowFactory.create(accountId,serialNumber,packageName,pageName,startTime,endTime);
    }
}

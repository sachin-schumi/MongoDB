package com.mongo;


import java.io.Serializable;
import java.util.Date;


/**
 * Created by manisha on 24/09/2016.
 */
//@UDT(keyspace = "thehood", name = "item")
public class Item {

    private int olItemId;
    private int olNumber;
    private int olSuppWarehouseId;
    private double olQuantity;
    private Date olDeliveryDate;
    private String olDistInfo;
    private double olAmount;

    public Item(int olItemId, int olNumber, int olSuppWarehouseId, double olQuantity, Date olDeliveryDate, String olDistInfo, double olAmount) {
        this.olItemId = olItemId;
        this.olNumber = olNumber;
        this.olSuppWarehouseId = olSuppWarehouseId;
        this.olQuantity = olQuantity;
        this.olDeliveryDate = olDeliveryDate;
        this.olDistInfo = olDistInfo;
        this.olAmount = olAmount;
    }

    public int getOlItemId() {
        return olItemId;
    }

    public void setOlItemId(int olItemId) {
        this.olItemId = olItemId;
    }

    public int getOlNumber() {
        return olNumber;
    }

    public void setOlNumber(int olNumber) {
        this.olNumber = olNumber;
    }

    public int getOlSuppWarehouseId() {
        return olSuppWarehouseId;
    }

    public void setOlSuppWarehouseId(int olSuppWarehouseId) {
        this.olSuppWarehouseId = olSuppWarehouseId;
    }

    public double getOlQuantity() {
        return olQuantity;
    }

    public void setOlQuantity(double olQuantity) {
        this.olQuantity = olQuantity;
    }

    public Date getOlDeliveryDate() {
        return olDeliveryDate;
    }

    public void setOlDeliveryDate(Date olDeliveryDate) {
        this.olDeliveryDate = olDeliveryDate;
    }

    public String getOlDistInfo() {
        return olDistInfo;
    }

    public void setOlDistInfo(String olDistInfo) {
        this.olDistInfo = olDistInfo;
    }

    public double getOlAmount() {
        return olAmount;
    }

    public void setOlAmount(double olAmount) {
        this.olAmount = olAmount;
    }

}

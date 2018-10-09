

/*
WARNING: THIS FILE IS AUTO-GENERATED. DO NOT MODIFY.

This file was generated from .idl using "rtiddsgen".
The rtiddsgen tool is part of the RTI Connext distribution.
For more information, type 'rtiddsgen -help' at a command shell
or consult the RTI Connext manual.
*/

package com.rti.dds.types;

import com.rti.dds.infrastructure.*;
import com.rti.dds.infrastructure.Copyable;
import java.io.Serializable;
import com.rti.dds.cdr.CdrHelper;

public class DataSample64B   implements Copyable, Serializable{

    public int source_id= 0;
    public int sample_id= 0;
    public long ts_milisec= 0;
    public int [] payload=  new int [12];

    public DataSample64B() {

    }
    public DataSample64B (DataSample64B other) {

        this();
        copy_from(other);
    }

    public static Object create() {

        DataSample64B self;
        self = new  DataSample64B();
        self.clear();
        return self;

    }

    public void clear() {

        source_id= 0;
        sample_id= 0;
        ts_milisec= 0;
        for(int i1__ = 0; i1__< 12; ++i1__){

            payload[i1__] =  0;
        }

    }

    public boolean equals(Object o) {

        if (o == null) {
            return false;
        }        

        if(getClass() != o.getClass()) {
            return false;
        }

        DataSample64B otherObj = (DataSample64B)o;

        if(source_id != otherObj.source_id) {
            return false;
        }
        if(sample_id != otherObj.sample_id) {
            return false;
        }
        if(ts_milisec != otherObj.ts_milisec) {
            return false;
        }
        for(int i1__ = 0; i1__< 12; ++i1__){

            if(payload[i1__] != otherObj.payload[i1__]) {
                return false;
            }
        }

        return true;
    }

    public int hashCode() {
        int __result = 0;
        __result += (int)source_id;
        __result += (int)sample_id;
        __result += (int)ts_milisec;
        for(int i1__ = 0; i1__< 12; ++i1__){

            __result += (int)payload[i1__];
        }

        return __result;
    }

    /**
    * This is the implementation of the <code>Copyable</code> interface.
    * This method will perform a deep copy of <code>src</code>
    * This method could be placed into <code>DataSample64BTypeSupport</code>
    * rather than here by using the <code>-noCopyable</code> option
    * to rtiddsgen.
    * 
    * @param src The Object which contains the data to be copied.
    * @return Returns <code>this</code>.
    * @exception NullPointerException If <code>src</code> is null.
    * @exception ClassCastException If <code>src</code> is not the 
    * same type as <code>this</code>.
    * @see com.rti.dds.infrastructure.Copyable#copy_from(java.lang.Object)
    */
    public Object copy_from(Object src) {

        DataSample64B typedSrc = (DataSample64B) src;
        DataSample64B typedDst = this;

        typedDst.source_id = typedSrc.source_id;
        typedDst.sample_id = typedSrc.sample_id;
        typedDst.ts_milisec = typedSrc.ts_milisec;
        System.arraycopy(typedSrc.payload,0,
        typedDst.payload,0,
        typedSrc.payload.length); 

        return this;
    }

    public String toString(){
        return toString("", 0);
    }

    public String toString(String desc, int indent) {
        StringBuffer strBuffer = new StringBuffer();        

        if (desc != null) {
            CdrHelper.printIndent(strBuffer, indent);
            strBuffer.append(desc).append(":\n");
        }

        CdrHelper.printIndent(strBuffer, indent+1);        
        strBuffer.append("source_id: ").append(source_id).append("\n");  
        CdrHelper.printIndent(strBuffer, indent+1);        
        strBuffer.append("sample_id: ").append(sample_id).append("\n");  
        CdrHelper.printIndent(strBuffer, indent+1);        
        strBuffer.append("ts_milisec: ").append(ts_milisec).append("\n");  
        CdrHelper.printIndent(strBuffer, indent+1);
        strBuffer.append("payload: ");
        for(int i1__ = 0; i1__< 12; ++i1__){

            strBuffer.append(payload[i1__]).append(", ");
        }

        strBuffer.append("\n");

        return strBuffer.toString();
    }

}

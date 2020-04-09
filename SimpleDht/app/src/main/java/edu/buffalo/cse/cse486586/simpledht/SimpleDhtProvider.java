package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;


import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;

public class SimpleDhtProvider extends ContentProvider {

    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    String successors = null;
    String predecessor = null;
    String myId = null;
    private static final ArrayList nodeList = new ArrayList();
    static final int SERVER_PORT = 10000;
    HashMap inputM= new HashMap();
    private ContentResolver mContentResolver;
    private ContentValues[] mContentValues;
    String portStr;
    ArrayList passon = null;

    private final Uri mUri  = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
    

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override   
        protected Void doInBackground(ServerSocket... serverSockets) {
            ServerSocket socket = serverSockets[0];
            String nodeDet = null;
            while (true) {
                System.out.println("Server Async Task");
                try {

                    Socket socketconnection = socket.accept();
                    System.out.println("Server Async Task socket accept");
                    DataInputStream inputstream = new DataInputStream(socketconnection.getInputStream());
                    System.out.println("Node  request recieved ");
                    String reqReceived = inputstream.readUTF();

                    System.out.println("Node  request read " + reqReceived);
                    String[] reqDet = reqReceived.split(":");
                    String reqType = reqDet[0];
                    if (reqType.equals("NodeAdd")) //New node addition request to 5554
                    {
                        //System.out.println("Node add request " + reqReceived);
                        String nodeEntered = genHash(reqDet[1]);
                        inputM.put(nodeEntered, reqDet[1]);
                        ArrayList temp = new ArrayList();// Temporary List to store previous list
                        temp.addAll(nodeList);
                        temp.add("DummyEntry"); // Dummy Entry to make length same
                        nodeList.add(nodeEntered);
                        Collections.sort(nodeList);
                        //System.out.println(nodeList.get(1));
                        for (int l = 0; l < nodeList.size(); l++) {
                            String currentN = (String) nodeList.get(l);
                            System.out.println("New " + nodeList.get(l) + " Old " + temp.get(l)+"Size "+nodeList.size());
                           // if (!temp.get(l).equals(nodeList.get(l))) {
                                //Predecessor and Successor change
                                if (l == 0) {
                                    //System.out.println("l=0");
                                    nodeDet = nodeList.get(nodeList.size() - 1) + ":" + inputM.get(nodeList.get(nodeList.size() - 1)) + ":" + currentN + ":" + nodeList.get(1) + ":" + inputM.get(nodeList.get(1));
                                    System.out.println(nodeDet);
                                } else if (l == nodeList.size() - 1) {
                                    nodeDet = nodeList.get(nodeList.size() - 2) + ":" + inputM.get(nodeList.get(nodeList.size() - 2)) + ":" + currentN + ":" + nodeList.get(0) + ":" + inputM.get(nodeList.get(0));
                                } else {
                                    nodeDet = nodeList.get(l - 1) + ":" + inputM.get(nodeList.get(l - 1)) + ":" + currentN + ":" + nodeList.get(l + 1) + ":" + inputM.get(nodeList.get(l + 1));

                                }
                                if (!nodeList.get(l).equals(nodeEntered)) {
                                    String port = (String) inputM.get(nodeList.get(l));
                                    //System.out.println("Port" + port);
                                    if (!port.equals("5554")) {
                                        int sPort = Integer.parseInt(port) * 2;
                                        //System.out.println("Sport " + sPort);
                                        Socket changeindexSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), sPort);
                                        if (changeindexSocket.isConnected()) {
                                            DataOutputStream outputStream1 = new DataOutputStream(changeindexSocket.getOutputStream());
                                            //System.out.println("Change " + nodeDet);
                                            outputStream1.writeUTF("ChangeRingLoc:" + nodeDet);
                                            outputStream1.flush();
                                        }
                                    } else {
                                        String[] splitnodeDetC = nodeDet.split(":");
                                        predecessor = splitnodeDetC[0];
                                        successors = splitnodeDetC[3];
                                        myId = splitnodeDetC[2];
                                        System.out.println("P " + inputM.get(predecessor) + " M " + inputM.get(myId) + " S " + inputM.get(successors));
                                    }
                                } else {
                                    //System.out.println(nodeDet);
                                    DataOutputStream outputstream = new DataOutputStream(socketconnection.getOutputStream());
                                    outputstream.writeUTF(nodeDet);
                                    outputstream.flush();
                                }
                           // }
                        }

                    } else if (reqType.equals("ChangeRingLoc")) {
                        //System.out.println("Node change request");
                        //String[] splitnodeDetC = reqDet[1].split(":");
                        predecessor = reqDet[1];
                        successors = reqDet[4];
                        myId = reqDet[3];
                        inputM.put(predecessor, reqDet[2]);
                        inputM.put(successors, reqDet[5]);
                        System.out.println("Node change P " + inputM.get(predecessor) + " M " + inputM.get(myId) + " S " + inputM.get(successors));
                    } else if (reqType.equals("InsertReq")) {
                        ContentValues cv = new ContentValues();

                        cv.put(KEY_FIELD, reqDet[1]);
                        cv.put(VALUE_FIELD, reqDet[2]);
                        getContext().getContentResolver().insert(mUri, cv);

                    } else if (reqType.equals("Delete")) {
                        Object[] keys = inputM.keySet().toArray();
                        if (reqDet[1].equals("5554*")) {
                            for (int d = 0; d < keys.length; d++) {
                                if ((!inputM.get(keys[d]).equals(portStr)) && (!inputM.get(keys[d]).equals(reqDet[2]))) {
                                    Socket deletefileSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(String.valueOf(inputM.get(keys[d]))) * 2);
                                    DataOutputStream outputStream2 = new DataOutputStream(deletefileSocket.getOutputStream());
                                    outputStream2.writeUTF("Delete:ALL");
                                    outputStream2.flush();
                                }
                                else if (inputM.get(keys[d]).equals(portStr))
                                {
                                    getContext().getContentResolver().delete(mUri, "@", null);
                                }
                            }
                        }
                        else if(reqDet[1].equals("ALL"))
                        {
                            getContext().getContentResolver().delete(mUri, "@", null);
                        }
                        else //Independent request
                        {
                            getContext().getContentResolver().delete(mUri, reqDet[1], null);
                        }
                    } else if (reqType.equals("Query")) {

                        if (reqDet[1].equals("5554*")) {
                            String globalMerge="";
                            Object[] keys = inputM.keySet().toArray();
                            for (int d = 0; d < keys.length; d++) {
                                System.out.println(inputM.get(keys[d]).equals(portStr)+" "+inputM.get(keys[d])+" "+portStr);
                                if ((!inputM.get(keys[d]).equals(portStr)) && (!inputM.get(keys[d]).equals(reqDet[2]))) {
                                    Socket queryfileSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(String.valueOf(inputM.get(keys[d]))) * 2);
                                    DataOutputStream outputStream2 = new DataOutputStream(queryfileSocket.getOutputStream());
                                    System.out.println("Write to servers");
                                    outputStream2.writeUTF("Query:" + "@");
                                    outputStream2.flush();
                                    System.out.println("5554 block");

                                    DataInputStream inputMs = new DataInputStream(queryfileSocket.getInputStream());
                                    globalMerge=globalMerge+inputMs.readUTF();
                                }
                            }

                            if(!reqDet[2].equals(portStr))
                            {
                                String[] fileList = getContext().fileList();
                                for (int f = 0; f < fileList.length; f++) {
                                        FileInputStream inputStream;
                                        inputStream = getContext().openFileInput(fileList[f]);
                                        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                                        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                                        StringBuffer stringBuffer = new StringBuffer();
                                        String line;
                                        while ((line = bufferedReader.readLine()) != null) {
                                            stringBuffer.append(line);
                                        }
                                        Object[] mRow = new Object[2];
                                        mRow[0] = fileList[f];
                                        mRow[1] = stringBuffer;
                                        globalMerge=globalMerge+mRow[0]+"#"+mRow[1]+":";
                                    }
                            }

                            DataOutputStream outputStreamQ = new DataOutputStream(socketconnection.getOutputStream());
                            System.out.println("Write to servers for *"+globalMerge);
                            outputStreamQ.writeUTF(globalMerge);
                            outputStreamQ.flush();

                        } else if (reqDet[1].equals("@")) // Bulk request from master
                        {
                            String[] fileList = getContext().fileList();
                            String resultMerge="";

                            try {

                                for (int f = 0; f < fileList.length; f++) {
                                    FileInputStream inputStream;
                                    inputStream = getContext().openFileInput(fileList[f]);
                                    InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                                    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                                    StringBuffer stringBuffer = new StringBuffer();
                                    String line;
                                    while ((line = bufferedReader.readLine()) != null) {
                                        stringBuffer.append(line);
                                    }
                                    Object[] mRow = new Object[2];
                                    mRow[0] = fileList[f];
                                    mRow[1] = stringBuffer;
                                    resultMerge=resultMerge+mRow[0]+"#"+mRow[1]+":";
                                }
                            } catch (IOException e) {
                            }
                            DataOutputStream outputStream2 = new DataOutputStream(socketconnection.getOutputStream());
                            System.out.println("Write to servers for *"+resultMerge);
                            outputStream2.writeUTF(resultMerge);
                            outputStream2.flush();

                        }


                    }
                    else if(reqDet[0].equals("QueryReq"))
                    {
                        if(reqDet[1].equals("5554")) {
                            //Calculate location
                            String resultQP="";
                            String keyLoc = genHash(reqDet[2]);
                            ArrayList tempQ = new ArrayList();
                            String portLoc = null;
                            tempQ.addAll(nodeList);
                            tempQ.add(keyLoc);
                            Collections.sort(tempQ);
                            int k = tempQ.indexOf(keyLoc);
                            if (k == 0 || k == tempQ.size() - 1) {
                                portLoc = String.valueOf(nodeList.get(0));
                            } else
                                portLoc = String.valueOf(tempQ.get(k + 1));
                            System.out.println(portLoc);
                            if(portLoc.equals(genHash("5554")))// In master
                            {
                                FileInputStream inputStreamUS;
                                inputStreamUS = getContext().openFileInput(reqDet[2]);
                                InputStreamReader inputStreamReader = new InputStreamReader(inputStreamUS);
                                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                                StringBuffer stringBuffer = new StringBuffer();
                                String line;
                                while ((line = bufferedReader.readLine()) != null) {
                                    stringBuffer.append(line);
                                }

                                Object[] mRow = new Object[2];
                                mRow[0] = reqDet[2];
                                mRow[1] = stringBuffer;
                                resultQP=mRow[0]+"#"+mRow[1];
                                DataOutputStream outputStreamQ1 = new DataOutputStream(socketconnection.getOutputStream());
                                System.out.println("Write to servers for 1 query"+resultQP);
                                outputStreamQ1.writeUTF(resultQP);
                                outputStreamQ1.flush();
                            }
                            else{
                            Socket query1Socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(String.valueOf(inputM.get(portLoc))) * 2);
                            DataOutputStream outputStream3 = new DataOutputStream(query1Socket.getOutputStream());
                            System.out.println("Write to servers");
                            outputStream3.writeUTF("QueryReq:Port:" + reqDet[2]);
                            outputStream3.flush();
                            System.out.println("5554 block");

                            DataInputStream inputMs = new DataInputStream(query1Socket.getInputStream());
                            resultQP=inputMs.readUTF();

                            DataOutputStream outputStreamQ1 = new DataOutputStream(socketconnection.getOutputStream());
                            System.out.println("Write to servers for 1 query"+resultQP);
                            outputStreamQ1.writeUTF(resultQP);
                            inputMs.close();
                            outputStreamQ1.flush();

                        }}
                        else if(reqDet[1].equals("Port"))
                        {
                            FileInputStream inputStreamUS;
                            inputStreamUS = getContext().openFileInput(reqDet[2]);
                            InputStreamReader inputStreamReader = new InputStreamReader(inputStreamUS);
                            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                            StringBuffer stringBuffer = new StringBuffer();
                            String line;
                            while ((line = bufferedReader.readLine()) != null) {
                                stringBuffer.append(line);
                            }

                            Object[] mRow = new Object[2];
                            mRow[0] = reqDet[2];
                            mRow[1] = stringBuffer;
                            DataOutputStream outputStreamQ1 = new DataOutputStream(socketconnection.getOutputStream());
                            outputStreamQ1.writeUTF(mRow[0]+"#"+mRow[1]);
                            outputStreamQ1.flush();
                            inputStreamUS.close();
                        }



                    }
                inputstream.close();

                } catch (UnknownHostException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }

        }
    }
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        try {

            if(selection.equals("@"))//Delete all local files
            {
                String[] fileList=getContext().fileList();
                for(int f=0;f<=fileList.length;f++)
                {
                    getContext().deleteFile(fileList[f]);
                }
            }
            else if(selection.equals("*"))//Delete all files stored in Ring DTH
            {   //Delete local first
                String[] fileList=getContext().fileList();
                for(int f=0;f<fileList.length;f++)
                {
                    Boolean delete = getContext().deleteFile(fileList[f]);
                    System.out.println(delete);
                }

                //Delete other files in Ring DTH
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt("11108"));
                if (socket.isConnected()) {
                    DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                    outputStream.writeUTF("Delete:" +"5554*");
                }
            }
            else {
                String hashKey=genHash(selection);
                if((myId.compareTo(predecessor)==0 && myId.compareTo(successors)==0) || (myId.compareTo(hashKey)>=0 && predecessor.compareTo(hashKey)<0))//store in local
                {
                    Boolean delete = getContext().deleteFile(selection);
                    System.out.println(delete);
                }
                else if( predecessor.compareTo(myId) > 0 && successors.compareTo(myId) > 0) {
                    System.out.println("Port +++ " + hashKey + "2nd case enter");
                    if (hashKey.compareTo(predecessor) >= 0 || hashKey.compareTo(myId) < 0) {

                        Boolean delete = getContext().deleteFile(selection);
                        System.out.println(delete);
                    } else {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(String.valueOf(inputM.get(successors))) * 2);

                        if (socket.isConnected()) {
                            DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                            outputStream.writeUTF("Delete:" +selection);
                        }
                    }
                }
                else //Pass to successor Server
                {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(String.valueOf(inputM.get(successors))) * 2);

                    if (socket.isConnected()) {
                        DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                        outputStream.writeUTF("Delete:" +selection);
                    }
                }


            }

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        System.out.println("insert");
        String filename = values.get(KEY_FIELD).toString();
        String string = values.get(VALUE_FIELD).toString();
        System.out.println("insert:"+filename+" "+string+":"+predecessor+":"+successors);
        try {
            String hashKey = genHash(filename);
            System.out.println("Port +++ " + hashKey + "1st case enter");
            if((myId.compareTo(predecessor)==0 && myId.compareTo(successors)==0) || (myId.compareTo(hashKey)>=0 && predecessor.compareTo(hashKey)<0))//store in local
            {    System.out.println("Port +++ " + hashKey + "1st case entered");
                FileOutputStream outputStream;
                outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                outputStream.write(string.getBytes());
                outputStream.close();
                Log.v("insert", values.toString());
            }
            else if( predecessor.compareTo(myId) > 0 && successors.compareTo(myId) > 0)
            {     System.out.println("Port +++ " + hashKey + "2nd case enter");
                if (hashKey.compareTo(predecessor) >= 0 || hashKey.compareTo(myId) < 0) {

                    System.out.println("Port +++ " + hashKey + "2nd case entered 1");
                    FileOutputStream outputStream;
                    outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                    outputStream.write(string.getBytes());
                    outputStream.close();
                    Log.v("insert", values.toString());
                }
                else {

                    //Forward to next
                    System.out.println("Port +++ " + hashKey + "2nd case entered 1");
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(String.valueOf(inputM.get(successors))) * 2);
                    if (socket.isConnected()) {
                        DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                        outputStream.writeUTF("InsertReq:" +filename+":"+string);
                    }

                }

            }
            else //Pass to successor Server
            {  System.out.println("Port +++ "+portStr + hashKey + "pass on");
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(String.valueOf(inputM.get(successors))) * 2);
                if (socket.isConnected()) {
                    DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                    outputStream.writeUTF("InsertReq:" +filename+":"+string);
                }
            }
            }
            catch (NoSuchAlgorithmException e)
            {
            e.printStackTrace();
            }
            catch (IOException e)
            {
            e.printStackTrace();
            }
            catch (Exception e)
            {
                System.out.println("Port +++ " +portStr+" "+e);
            Log.e(TAG, "File write failed");
            }


        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        Context context = getContext();
        TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        System.out.println(portStr);

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            if (portStr.equals("5554")) {
                String nodeHash = genHash(portStr);
                inputM.put(nodeHash,portStr);
                nodeList.add(nodeHash);
                myId=nodeHash;
                predecessor=nodeHash;
                successors=nodeHash;
            }else if (!portStr.equals("5554")) {
                String nodeHash = genHash(portStr);
                myId=nodeHash;
                predecessor=nodeHash;
                successors=nodeHash;
                System.out.println("Node add "+portStr);

                new nodeAdd().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, portStr);


            }

        }
        catch (IOException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return false;
    }
    private class nodeAdd extends AsyncTask<String, Void, Void> {
        String nodeDet=null;


        @Override
        protected Void doInBackground(String ...portStr) {
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt("11108"));
                //socket.setSoTimeout(1500);

                if (socket.isConnected()) {
                    DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                    //System.out.println("Send "+portStr[0]);
                    outputStream.writeUTF("NodeAdd:" + portStr[0]);

                    DataInputStream inputStream = new DataInputStream(socket.getInputStream());
                    nodeDet = inputStream.readUTF();
                    //System.out.println(nodeDet);

                    String[] splitnodeDetC = nodeDet.split(":");
                    predecessor = splitnodeDetC[0];
                    successors = splitnodeDetC[3];
                    myId = splitnodeDetC[2];
                    inputM.put(myId,portStr[0]);
                    inputM.put(predecessor,splitnodeDetC[1]);
                    inputM.put(successors,splitnodeDetC[4]);
                    System.out.println("in node add P " + inputM.get(predecessor) + " M " + inputM.get(myId) + " S " + inputM.get(successors));


                }
            } catch (IOException e) {
                e.printStackTrace();
            }
return null;
        }
    }


    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        /*
         * Reference: https://developer.android.com/reference/android/database/MatrixCursor#MatrixCursor(java.lang.String[])
         * Reference: https://developer.android.com/reference/android/database/MatrixCursor#addRow(java.lang.Object[])
         */
        try {
            System.out.println("Query: "+selection);
            if (selection.equals("@"))//Query all local files
            {
                String[] fileList = getContext().fileList();
                Cursor matrixCursor = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
                for (int f = 0; f < fileList.length; f++) {
                    FileInputStream inputStreamU;


                    try {
                        System.out.println("File name :  " + fileList[f]);
                        inputStreamU = getContext().openFileInput(fileList[f]);
                        InputStreamReader inputStreamReader = new InputStreamReader(inputStreamU);
                        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                        StringBuffer stringBuffer = new StringBuffer();
                        String line;
                        while ((line = bufferedReader.readLine()) != null) {
                            stringBuffer.append(line);
                        }

                        Object[] mRow = new Object[2];
                        mRow[0] = fileList[f];
                        mRow[1] = stringBuffer;
                        System.out.println("QueryResult@ : "+ mRow[0]+" "+ mRow[1]);
                        ((MatrixCursor) matrixCursor).addRow(mRow);
                        inputStreamU.close();
                    } catch (Exception e) {
                        Log.e(TAG, "File read failed");
                    }
                    Log.v("query", selection);

                }
                return matrixCursor;

            } else if (selection.equals("*"))//Query all files stored in Ring DTH
            {
                String globalFile = "";
                FileInputStream inputStreamU;
                String[] fileList = getContext().fileList();
                HashMap keyValue = new HashMap();
                Cursor matrixCursor = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
                for (int f = 0; f < fileList.length; f++) {
                   
                    try {
                        System.out.println("File name :  " + fileList[f]);
                        inputStreamU = getContext().openFileInput(fileList[f]);
                        InputStreamReader inputStreamReader = new InputStreamReader(inputStreamU);
                        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                        StringBuffer stringBuffer = new StringBuffer();
                        String line;
                        while ((line = bufferedReader.readLine()) != null) {
                            stringBuffer.append(line);
                        }

                        Object[] mRow = new Object[2];
                        mRow[0] = fileList[f];
                        mRow[1] = stringBuffer;
                        globalFile = globalFile + mRow[0] + "#" + mRow[1] + ":";
                        System.out.println("GLobal "+globalFile);
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                if (!predecessor.equals(myId)) {
                    //Query files in Ring DTH
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt("11108"));

                    if (socket.isConnected()) {
                        DataOutputStream outputStreamQM = new DataOutputStream(socket.getOutputStream());
                        outputStreamQM.writeUTF("Query:" + "5554*:"+portStr);

                        DataInputStream inputStreamQM = new DataInputStream(socket.getInputStream());
                        globalFile = globalFile + inputStreamQM.readUTF();

                        inputStreamQM.close();
                        outputStreamQM.flush();
                    }
                }

                String[] spiltG = globalFile.split(":");
                for (int s = 0; s < spiltG.length; s++) {
                    System.out.println(spiltG[s]);
                    Object[] mRow = new Object[2];
                    String[] splitKV = spiltG[s].split("#");
                    mRow[0] = splitKV[0];
                    mRow[1] = splitKV[1];
                    System.out.println("QueryResult* : "+ mRow[0]+" "+ mRow[1]);
                    ((MatrixCursor) matrixCursor).addRow(mRow);
                }

                return matrixCursor;
            } else {
              
                Cursor matrixCursor = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
               
                Boolean self=false;
                    try {
                       
                        System.out.println("File name :  " + selection);
                        //inputStreamU = getContext().openFileInput(selection);
                        String[] fileList = getContext().fileList();
                        for(int y=0;y<fileList.length;y++) {
                        if (fileList[y].equals(selection)) {
                            self = true;
                            break;
                        }
                            
                        }
                        System.out.println("File name : after break  " + selection);
                        if (!self) {

                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt("11108"));

                            if (socket.isConnected()) {
                                DataOutputStream outputStreamQM = new DataOutputStream(socket.getOutputStream());
                                outputStreamQM.writeUTF("QueryReq:5554:" + selection);

                                DataInputStream inputStreamQ = new DataInputStream(socket.getInputStream());
                                String cursorStr = inputStreamQ.readUTF();
                                Object[] mRow = new Object[2];
                                String[] splitKV = cursorStr.split("#");
                                mRow[0] = splitKV[0];
                                mRow[1] = splitKV[1];
                                ((MatrixCursor) matrixCursor).addRow(mRow);
                                inputStreamQ.close();
                                outputStreamQM.flush();
                            }
                            return matrixCursor;
                        } else {
                            FileInputStream inputStreamU;
                            inputStreamU = getContext().openFileInput(selection);
                            InputStreamReader inputStreamReader = new InputStreamReader(inputStreamU);
                            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                            StringBuffer stringBuffer = new StringBuffer();
                            String line;
                            while ((line = bufferedReader.readLine()) != null) {
                                stringBuffer.append(line);
                            }

                            Object[] mRow = new Object[2];
                            mRow[0] = selection;
                            mRow[1] = stringBuffer;
                            ((MatrixCursor) matrixCursor).addRow(mRow);
                            inputStreamU.close();
                            return matrixCursor;
                        }

                }
                    catch (IOException e) {
                        e.printStackTrace();
                    } catch (Exception e) {
                        Log.e(TAG, "File read failed");
                    }
            }
            }
        catch (IOException e) {
                        e.printStackTrace();
                    } catch (Exception e) {
                        Log.e(TAG, "File read failed");
                    }
        return null;
    }


    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }

        return formatter.toString();
    }
}

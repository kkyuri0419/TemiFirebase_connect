package com.example.firebase;

import android.os.AsyncTask;
import android.os.Bundle;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;

import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.database.ValueEventListener;

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import com.microsoft.azure.eventhubs.EventHubRuntimeInformation;
import com.microsoft.azure.eventhubs.EventPosition;
import com.microsoft.azure.eventhubs.PartitionReceiver;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import androidx.annotation.NonNull;
import androidx.appcompat.app.AppCompatActivity;

public class MainActivity extends AppCompatActivity {

    private TextView txtShow;
    private Button startbtn;
    private Button stopbtn;
    private static String msg;

    private static final String eventHubsCompatibleEndpoint = "sb://iothub-ns-xdk2iot-3630662-a0007a5c12.servicebus.windows.net/";
    private static final String eventHubsCompatiblePath = "xdk2iot";
    private static final String iotHubSasKey = "jnALouW45kFUOsnRx2bhfevWeBt4SNbNJUYtrf5dGGA=";
    private static final String iotHubSasKeyName = "service";

    private static final String temieventHubsCompatibleEndpoint = "sb://iothub-ns-myfirstiot-3544005-f2ad8a16fa.servicebus.windows.net/";
    private static final String temieventHubsCompatiblePath = "myfirstiothub123456";
    private static final String temiiotHubSasKey = "cFumEA2hFFSg618aoZrvHoJGsCi/eNoIyZpH0BD0jIM=";
    private static final String temiiotHubSasKeyName = "service";

    EventHubClient ehClient;
    ScheduledExecutorService scheduledexecutorService;

    EventHubClient temiehClient;
    ScheduledExecutorService temischeduledexecutorService;


    private static ArrayList<PartitionReceiver> receivers = new ArrayList<PartitionReceiver>();
    private static ArrayList<PartitionReceiver> temireceivers = new ArrayList<PartitionReceiver>();


    static DatabaseReference mRootRef = FirebaseDatabase.getInstance().getReference();
    static DatabaseReference conditionRef = mRootRef.child("text");
    static DatabaseReference connectionRef = mRootRef.child("connection");
    static DatabaseReference locationRef = mRootRef.child("location");

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        txtShow = findViewById(R.id.txtShow);
        startbtn = findViewById(R.id.startbtn);
        stopbtn = findViewById(R.id.stopbtn);
        startbtn.setEnabled(false);
        txtShow.setText("");

        ReadXDKAsyncTask readXDKAsyncTask = new ReadXDKAsyncTask();
        readXDKAsyncTask.execute();

        ReadTemiAsyncTask readTemiAsyncTask = new ReadTemiAsyncTask();
        readTemiAsyncTask.execute();


    }

    @Override
    protected void onStart() {
        super.onStart();

        conditionRef.addValueEventListener(new ValueEventListener() {
            @Override
            public void onDataChange(@NonNull DataSnapshot dataSnapshot) {
                String text = dataSnapshot.getValue(String.class);
                Log.e(this.getClass().getName(),"Updated");
                txtShow.setText(text);
            }

            @Override
            public void onCancelled(@NonNull DatabaseError databaseError) {

            }
        });
    }


    void stop() throws EventHubException {
        for (PartitionReceiver receiver : receivers) {
            receiver.closeSync();
        }
        ehClient.closeSync();
        scheduledexecutorService.shutdown();

        for (PartitionReceiver receiver : temireceivers) {
            receiver.closeSync();
        }
        temiehClient.closeSync();
        temischeduledexecutorService.shutdown();
    }
    public class ReadXDKAsyncTask extends AsyncTask {

        @Override
        protected Void doInBackground(Object... objects) {
            ConnectionStringBuilder connStr = null;
            try {
                connStr = new ConnectionStringBuilder()
                        .setEndpoint(new URI(eventHubsCompatibleEndpoint))
                        .setEventHubName(eventHubsCompatiblePath)
                        .setSasKeyName(iotHubSasKeyName)
                        .setSasKey(iotHubSasKey);
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }

            // Create an EventHubClient instance to connect to the
            // IoT Hub Event Hubs-compatible endpoint.
            scheduledexecutorService = Executors.newSingleThreadScheduledExecutor();
            try {
                ehClient = EventHubClient.createFromConnectionStringSync(connStr.toString(), scheduledexecutorService);
            } catch (EventHubException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

            // Use the EventHubRunTimeInformation to find out how many partitions
            // there are on the hub.
            EventHubRuntimeInformation eventHubInfo = null;
            try {
                eventHubInfo = ehClient.getRuntimeInformation().get();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // Create a PartitionReciever for each partition on the hub.
            for (String partitionId : eventHubInfo.getPartitionIds()) {
                try {
                    receiveMessages(ehClient, partitionId);
                } catch (EventHubException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            return null;
        }
    }

    public class ReadTemiAsyncTask extends AsyncTask {

        @Override
        protected Void doInBackground(Object... objects) {
            ConnectionStringBuilder connStr = null;
            try {
                connStr = new ConnectionStringBuilder()
                        .setEndpoint(new URI(temieventHubsCompatibleEndpoint))
                        .setEventHubName(temieventHubsCompatiblePath)
                        .setSasKeyName(temiiotHubSasKeyName)
                        .setSasKey(temiiotHubSasKey);
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }

            // Create an EventHubClient instance to connect to the
            // IoT Hub Event Hubs-compatible endpoint.
            temischeduledexecutorService = Executors.newSingleThreadScheduledExecutor();
            try {
                temiehClient = EventHubClient.createFromConnectionStringSync(connStr.toString(), scheduledexecutorService);
            } catch (EventHubException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

            // Use the EventHubRunTimeInformation to find out how many partitions
            // there are on the hub.
            EventHubRuntimeInformation eventHubInfo = null;
            try {
                eventHubInfo = temiehClient.getRuntimeInformation().get();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // Create a PartitionReciever for each partition on the hub.
            for (String partitionId : eventHubInfo.getPartitionIds()) {
                try {
                    temireceiveMessages(temiehClient, partitionId);
                } catch (EventHubException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            return null;
        }
    }


    private static void receiveMessages(EventHubClient ehClient, String partitionId)
            throws EventHubException, ExecutionException, InterruptedException {

        final ExecutorService executorService = Executors.newSingleThreadExecutor();

        // Create the receiver using the default consumer group.
        // For the purposes of this sample, read only messages sent since
        // the time the receiver is created. Typically, you don't want to skip any messages.
        ehClient.createReceiver(EventHubClient.DEFAULT_CONSUMER_GROUP_NAME, partitionId,
                EventPosition.fromEnqueuedTime(Instant.now())).thenAcceptAsync(receiver -> {
            System.out.println(String.format("Starting receive loop on partition: %s", partitionId));
            System.out.println(String.format("Reading messages sent since: %s", Instant.now().toString()));

            receivers.add(receiver);

            while (true) {
                try {
                    // Check for EventData - this methods times out if there is nothing to retrieve.
                    Iterable<EventData> receivedEvents = receiver.receiveSync(100);

                    // If there is data in the batch, process it.
                    if (receivedEvents != null) {
                        for (EventData receivedEvent : receivedEvents) {
                            System.out.println(String.format("Telemetry received:\n %s",
                                    new String(receivedEvent.getBytes(), Charset.defaultCharset())));
                            msg = new String(receivedEvent.getBytes(), Charset.defaultCharset());
                            conditionRef.setValue(msg);
                            System.out.println(String.format("Application properties (set by device):\n%s",receivedEvent.getProperties().toString()));
                            System.out.println(String.format("System properties (set by IoT Hub):\n%s\n",receivedEvent.getSystemProperties().toString()));
                        }
                    }
                } catch (EventHubException e) {
                    System.out.println("Error reading EventData");
                }
            }
        }, executorService);
    }

    private static void temireceiveMessages(EventHubClient ehClient, String partitionId)
            throws EventHubException, ExecutionException, InterruptedException {

        final ExecutorService executorService = Executors.newSingleThreadExecutor();

        // Create the receiver using the default consumer group.
        // For the purposes of this sample, read only messages sent since
        // the time the receiver is created. Typically, you don't want to skip any messages.
        ehClient.createReceiver(EventHubClient.DEFAULT_CONSUMER_GROUP_NAME, partitionId,
                EventPosition.fromEnqueuedTime(Instant.now())).thenAcceptAsync(receiver -> {
            System.out.println(String.format("Starting receive loop on partition: %s", partitionId));
            System.out.println(String.format("Reading messages sent since: %s", Instant.now().toString()));

            temireceivers.add(receiver);

            while (true) {
                try {
                    // Check for EventData - this methods times out if there is nothing to retrieve.
                    Iterable<EventData> receivedEvents = receiver.receiveSync(100);

                    // If there is data in the batch, process it.
                    if (receivedEvents != null) {
                        for (EventData receivedEvent : receivedEvents) {
                            System.out.println(String.format("Telemetry received:\n %s",
                                    new String(receivedEvent.getBytes(), Charset.defaultCharset())));
                            msg = new String(receivedEvent.getBytes(), Charset.defaultCharset());
                            if (msg.equals("connect")|msg.equals("disconnect")){
                                connectionRef.setValue(msg);
                            }else{
                                locationRef.setValue(msg);
                            }
                            System.out.println(String.format("Application properties (set by device):\n%s",receivedEvent.getProperties().toString()));
                            System.out.println(String.format("System properties (set by IoT Hub):\n%s\n",receivedEvent.getSystemProperties().toString()));
                        }
                    }
                } catch (EventHubException e) {
                    System.out.println("Error reading EventData");
                }
            }
        }, executorService);
    }

    public void startbtnOnClick(View view) {
        startbtn.setEnabled(false);
        stopbtn.setEnabled(true);
    }

    public void stopbtnOnClick(View view) throws EventHubException {
        stop();
        startbtn.setEnabled(true);
        stopbtn.setEnabled(false);
    }
}

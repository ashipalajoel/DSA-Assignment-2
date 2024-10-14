import ballerinax/kafka;
import ballerinax/mysql;
import ballerina/io;
import ballerinax/mysql.driver as _;
import ballerina/sql;

type Details record {
    string first_name;
    string last_name;
    string contact_number;
    string tracking_number ="";

    string shipment_type;
    string pickup_location;
    string delivery_location;
    string preferred_time_slot;
    
};

type Customer_Details record {
    @sql:Column {
        name: "id"
    }
    int id;
    @sql:Column {
        name: "first_name"
    }
    string first_name;
    @sql:Column {
        name: "last_name"
    }
    string last_name;
    @sql:Column {
        name: "contact_number"
    }
    string contact_number;
};

type Confirmation record {
    string status;
    string first_name;
    string estimated_Delivery_time;
    string delivery_location;
    string pickup_location;
    string shipment_type;
    string confirmation_id;
};

mysql:Client db = check new("localhost", "root", "Kalitheni@11", "logistics_db",3306);

listener kafka:Listener expressConsumer = check new(kafka:DEFAULT_URL, {
    groupId: "express-delivery-group",  
    topics: "express-delivery"
});

kafka:Producer confirmationProducer = check new(kafka:DEFAULT_URL);

service on expressConsumer {
    remote function onConsumerRecord(Details[] request) returns error? {
        foreach Details shipment_details in request {
            Confirmation confirmation = {
                status: "Confirmed",
                first_name: shipment_details.first_name,
                estimated_Delivery_time: "2 days",
                delivery_location: shipment_details.delivery_location,
                pickup_location: shipment_details.pickup_location,
                shipment_type: shipment_details.shipment_type,
                confirmation_id: shipment_details.tracking_number
            };
            check confirmationProducer->send({topic: "confirmationShipment", value: confirmation});

            io:println(shipment_details.first_name + " " + shipment_details.last_name + " " + shipment_details.contact_number + " " + shipment_details.tracking_number);
        }
    }
}

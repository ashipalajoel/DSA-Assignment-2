import ballerinax/kafka;
import ballerina/sql;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;
import ballerina/http;
import ballerina/io;
import ballerina/uuid;


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
    int id;
    string first_name;
    string last_name;
    string contact_number;
};

type Confirmation record {
    string status;
    string estimated_Delivery_time;
    string delivery_location;
    string pickup_location;
    string shipment_type;
    string confirmation_id;
};




listener kafka:Listener logisticConsumer = check new(kafka:DEFAULT_URL, {
    groupId: "logistic-delivery-group",
    topics: "confirmationShipment"
});

service /logistic_service on new http:Listener(9090) {
    private final mysql:Client db_client;
    private final kafka:Producer producer_logistic;

    function init() returns error?{
        self.producer_logistic = check new(kafka:DEFAULT_URL);
        self.db_client = check new ("localhost", "root", "Kalitheni@11", "logistics_db",3306);
    }

    resource function post sendPackage(Details request) returns string|error? {
        int customer_id = 0;
        sql:ParameterizedQuery custom_query = `SELECT * from Customers WHERE contact_number = ${request.contact_number}`;
        Customer_Details|sql:Error customer =  self.db_client->queryRow(custom_query);
        string tracking;
         if customer is sql:NoRowsError{
            sql:ParameterizedQuery add_customer = `INSERT INTO Customers (first_name, last_name, contact_number) VALUES (${request.first_name},${request.last_name},${request.contact_number})`;
            sql:ExecutionResult _ = check self.db_client->execute(add_customer);

            sql:ParameterizedQuery id_query = `SELECT id FROM Customers WHERE contact_number = ${request.contact_number}`;
            Customer_Details new_customer = check self.db_client->queryRow(id_query);
            customer_id = new_customer.id;
            
            tracking = uuid:createType1AsString();
            sql:ParameterizedQuery add_shipments = `INSERT INTO Shipments (customer_id, shipment_type, pickup_location, delivery_location, preferred_time_slot,tracking_number) VALUES (${customer_id},${request.shipment_type},${request.pickup_location},${request.delivery_location},${request.preferred_time_slot}, ${tracking})`;
            sql:ExecutionResult _ = check self.db_client->execute(add_shipments);
        }

        else {
            sql:ParameterizedQuery id_query = `SELECT id FROM Customers WHERE contact_number = ${request.contact_number}`;
            Customer_Details get_customer = check self.db_client->queryRow(id_query);
            customer_id =  get_customer.id;
            tracking = uuid:createType1AsString();
            sql:ParameterizedQuery add_shipments = `INSERT INTO Shipments (customer_id, shipment_type, pickup_location, delivery_location, preferred_time_slot,tracking_number) VALUES (${customer_id},${request.shipment_type},${request.pickup_location},${request.delivery_location},${request.preferred_time_slot}, ${tracking})`;
            sql:ExecutionResult _ = check self.db_client->execute(add_shipments); 
               
        }

        request.tracking_number =tracking;
        
        check self.producer_logistic->send({topic: request.shipment_type, value:request});
        return string `Package sent for tracking number ${tracking}`;
    }
}

service on logisticConsumer {

    private final mysql:Client db_client; 

    function init() returns error?{
        self.db_client = check new ("localhost", "root", "Kalitheni@11", "logistics_db",3306);
    }
    remote function onConsumerRecord(Confirmation[] confirm_req) returns error? {
        Details shipment;
        foreach Confirmation request in confirm_req {
            sql:ParameterizedQuery check_shipments = `SELECT * FROM Shipments WHERE tracking_number = ${request.confirmation_id}`;
            shipment = check self.db_client->queryRow(check_shipments);
            io:println("Shipment details: " + shipment.first_name + " " + shipment.last_name + " " + shipment.contact_number + " " + shipment.shipment_type + " " + shipment.pickup_location + " " + shipment.delivery_location + " " + shipment.preferred_time_slot + " " + shipment.tracking_number);
            
        }
    }


}
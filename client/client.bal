import ballerina/io;
import ballerina/http;

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

http:Client http_client = check new ("localhost:9090/logistic_service");

public function main() returns error?{
    Details request = {
        first_name: io:readln("Enter your first name: "),
        last_name: "OMa",
        contact_number: io:readln("Enter your contact number: "),
        shipment_type: "standard-delivery", 
        pickup_location: "Windhoek",
        delivery_location: "Oranjemund",
        preferred_time_slot: "morning"
    };
    string response = check http_client->/sendPackage.post(request);

    io:println(response);

}

function shipment_type(string shipment_type) returns string {
    if (shipment_type == "standard-delivery") {
        return shipment_type;
    }
    else if (shipment_type == "express-delivery") {
        return shipment_type;
    }
    else if (shipment_type == "international-delivery") {
        return shipment_type;
    }
    else {
        return "Invalid shipment type";
    }
}

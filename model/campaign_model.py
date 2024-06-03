from dataclasses import dataclass
from typing import Optional

@dataclass
class InteractiveInsightsDTO:
    query: str
    

    print("Inside InteractiveInsightsDTO")

    @staticmethod
    def validatePayload(payload):
        expected_keys = {'query'}
        missing_keys = [key for key in expected_keys if key not in payload]
        if missing_keys:
            print(f"Missing keys: {missing_keys}")
            return False
        print("Payload is valid")
        return True

    @staticmethod
    def instance_from_flask_body(data: dict) -> 'InteractiveInsightsDTO':
        if 'query' not in data:
            raise ValueError('query not found')


        return InteractiveInsightsDTO(
            query=data['query']
            
        )

async function loadContract(){

    const response = await fetch("./config/runtime_contract_v1.json");
    return await response.json();
}

async function validatePayload(payload){

    const contract = await loadContract();

    const missing = [];

    for(const field of contract.required_fields){

        if(!(field in payload)){
            missing.push(field);
        }
    }

    return {
        valid: missing.length === 0,
        missing_fields: missing,
        governance: contract.governance
    };
}

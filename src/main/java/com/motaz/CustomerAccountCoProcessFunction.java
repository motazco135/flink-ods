package com.motaz;

import com.motaz.dto.AccountDto;
import com.motaz.dto.CustomerAccountDto;
import com.motaz.dto.CustomerDto;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class CustomerAccountCoProcessFunction extends CoProcessFunction<CustomerDto, AccountDto, CustomerAccountDto> {

    // State to hold the latest CustomerDto and AccountDto for each customer ID
    private transient ValueState<CustomerDto> customerState;
    private transient ValueState<AccountDto> accountState;

    @Override
    public void open(Configuration parameters) {
        customerState = getRuntimeContext().getState(new ValueStateDescriptor<>("customerState", CustomerDto.class));
        accountState = getRuntimeContext().getState(new ValueStateDescriptor<>("accountState", AccountDto.class));
    }

    @Override
    public void processElement1(CustomerDto customer, CoProcessFunction<CustomerDto, AccountDto, CustomerAccountDto>.Context ctx, Collector<CustomerAccountDto> out) throws Exception {
        customerState.update(customer);
        // Attempt to join with the latest account
        AccountDto account = accountState.value();
        if (account != null) {
            out.collect(createDenormalized(customer, account));
        }
    }

    @Override
    public void processElement2(AccountDto account, CoProcessFunction<CustomerDto, AccountDto, CustomerAccountDto>.Context ctx, Collector<CustomerAccountDto> out) throws Exception {
        accountState.update(account);
        // Attempt to join with the latest customer
        CustomerDto customer = customerState.value();
        if (customer != null) {
            out.collect(createDenormalized(customer, account));
        }
    }

    private CustomerAccountDto createDenormalized(CustomerDto customer, AccountDto account) {
        CustomerAccountDto denormalized = new CustomerAccountDto();
        // Set fields from CustomerDto
        denormalized.setCustomerId(customer.getId());
        denormalized.setCustomerName(customer.getFirstName().concat(" ").concat(customer.getLastName()));
        denormalized.setEmail(customer.getEmail());
        denormalized.setMobile(customer.getMobile());

        // Set fields from AccountDto
        denormalized.setAccountId(account.getId());
        denormalized.setAccountNumber(account.getAccountNumber().toString());
        denormalized.setAccountStatus(account.getAccountStatus());
        denormalized.setAccountBalance(account.getAccountBalance());

        int diff = customer.getUpdatedAt().compareTo(account.getUpdatedAt());
        if(diff>0){
            denormalized.setUpdatedAt(customer.getUpdatedAt());
        }else if (diff < 0) {
            denormalized.setUpdatedAt(account.getUpdatedAt());
        }else{ //dates are equal
            denormalized.setUpdatedAt(account.getUpdatedAt());
        }


        return denormalized;
    }
}

import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder
import org.springframework.security.crypto.password.PasswordEncoder

import org.apache.activemq.ActiveMQConnectionFactory
import org.springframework.jms.connection.SingleConnectionFactory

import com.grailsinaction.MarshallerRegistrar

// Place your Spring DSL code here
beans = {
    passwordEncoder(BCryptPasswordEncoder) // Defines a BCryptPasswordEncoder bean

    hubbubMarshallerRegistrar(MarshallerRegistrar)

    jmsConnectionFactory(SingleConnectionFactory) {
        targetConnectionFactory = { ActiveMQConnectionFactory cf ->
            brokerURL = "vm://localhost"
        }
    }
}

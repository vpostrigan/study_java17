import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder
import org.springframework.security.crypto.password.PasswordEncoder

import com.grailsinaction.MarshallerRegistrar

// Place your Spring DSL code here
beans = {
    passwordEncoder(BCryptPasswordEncoder) // Defines a BCryptPasswordEncoder bean

    hubbubMarshallerRegistrar(MarshallerRegistrar)
}

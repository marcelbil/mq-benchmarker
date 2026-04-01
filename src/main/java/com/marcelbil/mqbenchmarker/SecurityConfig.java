package com.marcelbil.mqbenchmarker;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
@EnableWebSecurity
public class SecurityConfig {

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http
            .csrf(csrf -> csrf.disable())
            .authorizeHttpRequests(auth -> auth
                .requestMatchers("/login.html", "/css/**", "/js/**").permitAll() 
                .anyRequest().authenticated() 
            )
            .formLogin(form -> form
                .loginPage("/login.html") 
                .loginProcessingUrl("/login") 
                .defaultSuccessUrl("/index.html", true) 
                .permitAll()
            )
            .logout(logout -> logout.permitAll());

        return http.build();
    }

    // NIEUW: Onze hardcoded "In-Memory" gebruiker
    @Bean
    public UserDetailsService userDetailsService() {
        UserDetails admin = User.builder()
            .username("admin")
            // De {noop} tag vertelt Spring dat we het wachtwoord onversleuteld (plain-text) opslaan. 
            // Voor een productie-omgeving zou hier een BCrypt hash staan.
            .password("{noop}benchmark") 
            .roles("ADMIN", "USER")
            .build();

        // Je kunt hier makkelijk meerdere gebruikers toevoegen als je wilt:
        // UserDetails gast = User.builder().username("gast").password("{noop}welkom").roles("USER").build();
        // return new InMemoryUserDetailsManager(admin, gast);

        return new InMemoryUserDetailsManager(admin);
    }
}
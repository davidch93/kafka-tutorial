package com.dch.tutorial.kafka.model;

import java.io.Serializable;

/**
 * User model.
 * 
 * @author David.Christianto
 */
public class User implements Serializable {

	private static final long serialVersionUID = -1209183511861019727L;

	private Long id;
	private String firstName;
	private String lastName;
	private String email;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	@Override
	public String toString() {
		return "User [id=" + id + ", firstName=" + firstName + ", lastName=" + lastName + ", email=" + email + "]";
	}
}

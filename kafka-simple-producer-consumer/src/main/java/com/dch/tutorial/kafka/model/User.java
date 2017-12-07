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
	private String name;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String toString() {
		return "User [id=" + id + ", name=" + name + "]";
	}
}

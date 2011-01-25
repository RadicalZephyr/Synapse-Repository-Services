package org.sagebionetworks.repo.model.gaejdo;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

import com.google.appengine.api.datastore.Key;

/**
 * This is the persistable class for a Annotations whose values are Dates
 * 
 * Note:  equals and hashcode are based on the attribute and value, allowing 
 * distinct annotations with the same attribute.
 * 
 * @author bhoff
 * 
 */
@PersistenceCapable(detachable = "true")
public class GAEJDODateAnnotation implements GAEJDOAnnotation<Date> {
	@PrimaryKey
	@Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
	private Key id;

	@Persistent
	private GAEJDOAnnotations owner; // this is the backwards pointer for the
									// 1-1 owned relationship

	@Persistent
	private String attribute;

	private Date value;

	public GAEJDODateAnnotation() {
	}

	public GAEJDODateAnnotation(String attr, Date value) {
		setAttribute(attr);
		setValue(value);
	}

	private static final DateFormat df = new SimpleDateFormat("ddMMMyyyy HH:mm:ss.");
	public String toString() {
		return getAttribute() + ": " + df.format(getValue());
	}
	public Key getId() {
		return id;
	}

	public void setId(Key id) {
		this.id = id;
	}

	 public GAEJDOAnnotations getOwner() {
	 return owner;
	 }
	
	 public void setOwner(GAEJDOAnnotations owner) {
	 this.owner = owner;
	 }

	public String getAttribute() {
		return attribute;
	}

	public void setAttribute(String attribute) {
		this.attribute = attribute;
	}

	public Date getValue() {
		return value;
	}

	public void setValue(Date value) {
		this.value = value;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((attribute == null) ? 0 : attribute.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		GAEJDODateAnnotation other = (GAEJDODateAnnotation) obj;
		if (attribute == null) {
			if (other.attribute != null)
				return false;
		} else if (!attribute.equals(other.attribute))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

}

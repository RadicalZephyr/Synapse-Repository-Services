package org.sagebionetworks.web.client.view;

import java.util.List;

import org.sagebionetworks.web.client.PlaceChanger;
import org.sagebionetworks.web.client.SynapsePresenter;
import org.sagebionetworks.web.client.SynapseView;

import com.google.gwt.user.client.ui.IsWidget;

public interface DatasetsHomeView extends IsWidget, SynapseView {
	
	/**
	 * Set this view's presenter
	 * @param presenter
	 */
	public void setPresenter(Presenter presenter);
	
	public void setVisibleColumns(List<String> visible);
		
	public interface Presenter extends SynapsePresenter {
		
		/**
		 * Called when the edit columns button is pushed.
		 */
		public void onEditColumns();
		
	}

}
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.CatalogHandlers;
import org.apache.iceberg.rest.requests.ListNamespacesRequest;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.RESTMessage;
import org.apache.iceberg.rest.RESTRequest;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.RESTException;
import org.apache.iceberg.util.Pair;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

public class TestCatalogHandlers {

  @Test
  public void testListNamespacesWithoutPageToken() {
        // Mock a REST request without pageToken query param (simulates null)
        RESTRequest mockRequest = Mockito.mock(RESTRequest.class);
        when(mockRequest.uri()).thenReturn("/v1/namespaces");  // Base path
        when(mockRequest.method()).thenReturn("GET");
        when(mockRequest.queryParams()).thenReturn(Map.of());  // No params, so pageToken null

        // Mock the ListNamespacesRequest parsed from the mock (with parent null, pageToken null)
        ListNamespacesRequest listRequest = ListNamespacesRequest.builder().withParent(null).build();

        // Call the handler (assuming a mock catalog or use TestCatalog if available)
        // Note: In full test, inject a real or mock BaseCatalog
        assertThatThrownBy(() -> CatalogHandlers.listNamespaces(/* mockCatalog */ null, listRequest))
                .isInstanceOf(RESTException.class)  // Or specific exception from the null check failure
                .hasMessageContaining("Invalid page token");  // Adjust based on actual error
    }
  
}

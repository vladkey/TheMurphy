package murphy.appmaster;

import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpServer;


import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;



public class WebUI {

	private AppState state;

	public WebUI(AppState exported) {
		this.state = exported;
	}

	public void start(InetSocketAddress listenOn) throws IOException {
		HttpServer server = HttpServer.create(listenOn, 0);
		HttpContext context = server.createContext("/murphy");
		context.setHandler((he) -> {
			String uri = he.getRequestURI().getPath();
			// currently just always print the state; handle POST / actions next step
			String response = serializeAppState(uri);

			byte[] respBytes = response.getBytes("UTF-8");
			he.sendResponseHeaders(200, respBytes.length);
			he.getResponseHeaders().set("Content-Type", "text/html");
			final OutputStream output = he.getResponseBody();
			output.write(respBytes);
			output.flush();
			he.close();
		});
		server.start();
	}

	public String serializeAppState(String uri) {
		String r = "<html><body>";
		r += "URI passed: " + uri;
		r += "<table>\n" +
				"<tr><td>Container Id</td><td>Status</td></tr>";
		for (long contId : state.container2task.keySet()) {
			r += "<tr><td>" + contId + "</td><td>" + state.container2task.get(contId) + "</td></tr>\n";
		}
		r += "<form target='/murphy/api' method='POST'>" +
		     "<input type='text' value='1050000000' name='memSize' />" +
		     "<input type='hidden' value='create' name='action' />" +
		     "<input type='submit' value='Request new container' name='btnSubmit' />" +
		     "</form>";
		r += "</table></body></html>";
		return r;
	}
}

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
			String payload = serializeAppState();
			// he.getRequestURI()
			he.sendResponseHeaders(200, payload.getBytes().length);
			final OutputStream output = he.getResponseBody();
			output.write(payload.getBytes());
			output.flush();
			he.close();
		});
		server.start();
	}

	public String serializeAppState() {
		return state.toString();
	}
}

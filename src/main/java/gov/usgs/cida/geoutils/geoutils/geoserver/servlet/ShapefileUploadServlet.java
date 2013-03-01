package gov.usgs.cida.geoutils.geoutils.geoserver.servlet;

import gov.usgs.cida.owsutils.commons.communication.RequestResponseHelper;
import gov.usgs.cida.owsutils.commons.io.FileHelper;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.*;
import org.apache.commons.codec.binary.Base64OutputStream;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.LoggerFactory;

public class ShapefileUploadServlet extends HttpServlet {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ShapefileUploadServlet.class);
    private static final long serialVersionUID = 1L;
    private static Integer maxFileSize;
    private static String filenameParam;

    @Override
    public void init(ServletConfig servletConfig) {
        Integer defaultMaxFileSize = Integer.MAX_VALUE;
        String defaultFilenameParam = "qqfile";

        String mfsInitParm = servletConfig.getInitParameter("maxFileSize");
        filenameParam = servletConfig.getInitParameter("filenameParam");

        if (StringUtils.isBlank(mfsInitParm)) {
            maxFileSize = defaultMaxFileSize;
        }

        try {
            maxFileSize = Integer.parseInt(mfsInitParm);
        } catch (NumberFormatException nfe) {
            maxFileSize = defaultMaxFileSize;
        }

        if (StringUtils.isBlank(filenameParam)) {
            filenameParam = defaultFilenameParam;
        }

    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, FileNotFoundException {
        doPost(request, response);
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, FileNotFoundException {
        Map<String, String> responseMap = new HashMap<String, String>();
        int fileSize = Integer.parseInt(request.getHeader("Content-Length"));
        if (fileSize > maxFileSize) {
            responseMap.put("error", "Upload exceeds max file size of " + maxFileSize + " bytes");
            RequestResponseHelper.sendErrorResponse(response, responseMap, RequestResponseHelper.ResponseType.XML);
            return;
        }
        
        String filename = request.getParameter(filenameParam);
        String responseEncoding = request.getParameter("response-encoding");
        String utilityWpsUrl = request.getParameter("utilitywps");
        String wfsEndpoint = request.getParameter("wfs-url");
        String tempDir = System.getProperty("java.io.tmpdir");
        File tempFile = new File(tempDir + File.separator + filename);
        
        RequestResponseHelper.ResponseType responseType = RequestResponseHelper.ResponseType.XML;
        
        if(StringUtils.isBlank(responseEncoding) || responseEncoding.toLowerCase().contains("json")) {
           responseType = RequestResponseHelper.ResponseType.JSON;
        }
        LOG.trace("Response type set to " + responseType.toString());
        
        try {
            RequestResponseHelper.saveFileFromRequest(request, tempFile, filenameParam);
            LOG.trace("File saved to " + tempFile.getPath());
            
            FileHelper.flattenZipFile(tempFile.getPath());
            LOG.trace("Zip file directory structure flattened");
            
            if (!FileHelper.validateShapefileZip(tempFile)) {
                throw new IOException("Unable to verify shapefile. Upload failed.");
            }
            LOG.trace("Zip file seems to be a valid shapefile");
            
        } catch (FileUploadException ex) {
            LOG.warn(ex.getMessage());
            responseMap.put("error", "Unable to upload file");
            responseMap.put("exception", ex.getMessage());
            RequestResponseHelper.sendErrorResponse(response, responseMap, responseType);
            return;
        } catch (IOException ex) {
            LOG.warn(ex.getMessage());
            responseMap.put("error", "Unable to upload file");
            responseMap.put("exception", ex.getMessage());
            RequestResponseHelper.sendErrorResponse(response, responseMap, responseType);
            return;
        }

        try {
            String wpsResponse = postToWPS(utilityWpsUrl, wfsEndpoint, tempFile);
            responseMap.put("wpsResponse", "<![CDATA[" + wpsResponse + "]]>");
        } catch (Exception ex) {
            LOG.warn(ex.getMessage());
            responseMap.put("error", "Unable to upload file");
            responseMap.put("exception", ex.getMessage());
            RequestResponseHelper.sendErrorResponse(response, responseMap, RequestResponseHelper.ResponseType.XML);
            return;
        } finally {
            FileUtils.deleteQuietly(tempFile);
        }
        
        LOG.trace("Shapefile has been imported successfully");
        RequestResponseHelper.sendSuccessResponse(response, responseMap, RequestResponseHelper.ResponseType.XML);
    }

    private String postToWPS(String url, String wfsEndpoint, File uploadedFile) throws IOException {
        HttpPost post;
        HttpClient httpClient = new DefaultHttpClient();

        post = new HttpPost(url);

        File wpsRequestFile = createWPSReceiveFilesXML(uploadedFile, wfsEndpoint);
        FileInputStream wpsRequestInputStream = null;
        try {
            wpsRequestInputStream = new FileInputStream(wpsRequestFile);

            AbstractHttpEntity entity = new InputStreamEntity(wpsRequestInputStream, wpsRequestFile.length());

            post.setEntity(entity);

            HttpResponse response = httpClient.execute(post);

            return EntityUtils.toString(response.getEntity());

        } finally {
            IOUtils.closeQuietly(wpsRequestInputStream);
            FileUtils.deleteQuietly(wpsRequestFile);
        }
    }

    private static File createWPSReceiveFilesXML(final File uploadedFile, final String wfsEndpoint) throws IOException {

        File wpsRequestFile = null;
        FileOutputStream wpsRequestOutputStream = null;
        FileInputStream uploadedInputStream = null;

        try {
            wpsRequestFile = File.createTempFile("wps.upload.", ".xml");
            wpsRequestOutputStream = new FileOutputStream(wpsRequestFile);
            uploadedInputStream = new FileInputStream(uploadedFile);

            wpsRequestOutputStream.write(new String(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                    + "<wps:Execute service=\"WPS\" version=\"1.0.0\" "
                    + "xmlns:wps=\"http://www.opengis.net/wps/1.0.0\" "
                    + "xmlns:ows=\"http://www.opengis.net/ows/1.1\" "
                    + "xmlns:xlink=\"http://www.w3.org/1999/xlink\" "
                    + "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" "
                    + "xsi:schemaLocation=\"http://www.opengis.net/wps/1.0.0 "
                    + "http://schemas.opengis.net/wps/1.0.0/wpsExecute_request.xsd\">"
                    + "<ows:Identifier>gov.usgs.cida.gdp.wps.algorithm.filemanagement.ReceiveFiles</ows:Identifier>"
                    + "<wps:DataInputs>"
                    + "<wps:Input>"
                    + "<ows:Identifier>filename</ows:Identifier>"
                    + "<wps:Data>"
                    + "<wps:LiteralData>"
                    + StringEscapeUtils.escapeXml(uploadedFile.getName().replace(".zip", ""))
                    + "</wps:LiteralData>"
                    + "</wps:Data>"
                    + "</wps:Input>"
                    + "<wps:Input>"
                    + "<ows:Identifier>wfs-url</ows:Identifier>"
                    + "<wps:Data>"
                    + "<wps:LiteralData>"
                    + StringEscapeUtils.escapeXml(wfsEndpoint)
                    + "</wps:LiteralData>"
                    + "</wps:Data>"
                    + "</wps:Input>"
                    + "<wps:Input>"
                    + "<ows:Identifier>file</ows:Identifier>"
                    + "<wps:Data>"
                    + "<wps:ComplexData mimeType=\"application/x-zipped-shp\" encoding=\"Base64\">").getBytes());
            IOUtils.copy(uploadedInputStream, new Base64OutputStream(wpsRequestOutputStream, true, 0, null));
            wpsRequestOutputStream.write(new String(
                    "</wps:ComplexData>"
                    + "</wps:Data>"
                    + "</wps:Input>"
                    + "</wps:DataInputs>"
                    + "<wps:ResponseForm>"
                    + "<wps:ResponseDocument>"
                    + "<wps:Output>"
                    + "<ows:Identifier>result</ows:Identifier>"
                    + "</wps:Output>"
                    + "<wps:Output>"
                    + "<ows:Identifier>wfs-url</ows:Identifier>"
                    + "</wps:Output>"
                    + "<wps:Output>"
                    + "<ows:Identifier>featuretype</ows:Identifier>"
                    + "</wps:Output>"
                    + "</wps:ResponseDocument>"
                    + "</wps:ResponseForm>"
                    + "</wps:Execute>").getBytes());
        } finally {
            IOUtils.closeQuietly(wpsRequestOutputStream);
            IOUtils.closeQuietly(uploadedInputStream);
        }
        return wpsRequestFile;
    }
}

package gov.usgs.cida.geoutils.geoutils.geoserver.servlet;

import gov.usgs.cida.config.DynamicReadOnlyProperties;
import gov.usgs.cida.owsutils.commons.communication.RequestResponseHelper;
import gov.usgs.cida.owsutils.commons.io.FileHelper;
import gov.usgs.cida.owsutils.commons.properties.JNDISingleton;
import it.geosolutions.geoserver.rest.GeoServerRESTManager;
import it.geosolutions.geoserver.rest.GeoServerRESTPublisher;
import it.geosolutions.geoserver.rest.GeoServerRESTPublisher.UploadMethod;
import it.geosolutions.geoserver.rest.GeoServerRESTReader;
import it.geosolutions.geoserver.rest.encoder.GSResourceEncoder;
import it.geosolutions.geoserver.rest.encoder.GSResourceEncoder.ProjectionPolicy;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.LoggerFactory;

public class ShapefileUploadServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ShapefileUploadServlet.class);
    private static DynamicReadOnlyProperties props = null;
    private static Integer maxFileSize;
    private static String filenameParam;
    private static String geoserverEndpoint;
    private static URL geoserverEndpointURL;
    private static String geoserverUsername;
    private static String geoserverPassword;
    private static GeoServerRESTManager gsRestManager;
    
    // Defaults
    private static String defaultWorkspaceName;
    private static String defaultStoreName;
    private static String defaultSRS;
    private static String defaultFilenameParam = "qqfile"; // Legacy to handle jquery fineuploader
    private static Integer defaultMaxFileSize = Integer.MAX_VALUE;

    @Override
    public void init(ServletConfig servletConfig) throws ServletException {
        super.init();
        props = JNDISingleton.getInstance();

        String mfsInitParam = servletConfig.getInitParameter("max-file-size");
        String mfsJndiProp = props.getProperty("max-file-size", "");
        if (StringUtils.isNotBlank(mfsInitParam)) {
            maxFileSize = Integer.parseInt(mfsInitParam);
        } else if (StringUtils.isNotBlank(mfsJndiProp)) {
            maxFileSize = Integer.parseInt(mfsJndiProp);
        } else {
            maxFileSize = defaultMaxFileSize;
        }
        LOG.trace("Maximum allowable file size set to: " + maxFileSize + " bytes");

        String fnInitParam = servletConfig.getInitParameter("filename-param");
        String fnJndiProp = props.getProperty("filename-param", "");
        if (StringUtils.isNotBlank(fnInitParam)) {
            filenameParam = fnInitParam;
        } else if (StringUtils.isNotBlank(fnJndiProp)) {
            filenameParam = fnJndiProp;
        } else {
            filenameParam = defaultFilenameParam;
        }
        LOG.trace("Filename parameter set to: " + filenameParam);

        String gsepInitParam = servletConfig.getInitParameter("geoserver-endpoint");
        String gsepJndiProp = props.getProperty("geoserver-endpoint");
        if (StringUtils.isNotBlank(gsepInitParam)) {
            geoserverEndpoint = gsepInitParam;
        } else if (StringUtils.isNotBlank(gsepJndiProp)) {
            geoserverEndpoint = gsepJndiProp;
        } else {
            throw new ServletException("Geoserver endpoint is not defined.");
        }
        LOG.trace("Geoserver endpoint set to: " + geoserverEndpoint);

        try {
            geoserverEndpointURL = new URL(geoserverEndpoint);
        } catch (MalformedURLException ex) {
            throw new ServletException("Geoserver endpoint (" + geoserverEndpoint + ") could not be parsed into a valid URL.");
        }

        String gsuserInitParam = servletConfig.getInitParameter("geoserver-username");
        String gsuserJndiProp = props.getProperty("geoserver-username");
        if (StringUtils.isNotBlank(gsuserInitParam)) {
            geoserverUsername = gsepInitParam;
        } else if (StringUtils.isNotBlank(gsuserJndiProp)) {
            geoserverUsername = gsepJndiProp;
        } else {
            throw new ServletException("Geoserver username is not defined.");
        }
        LOG.trace("Geoserver username set to: " + geoserverUsername);

        String gspassJndiProp = props.getProperty("geoserver-password");
        if (StringUtils.isNotBlank(gspassJndiProp)) {
            geoserverPassword = gsepJndiProp;
        } else {
            throw new ServletException("Geoserver password is not defined.");
        }
        LOG.trace("Geoserver password is set");

        try {
            gsRestManager = new GeoServerRESTManager(geoserverEndpointURL, geoserverUsername, geoserverPassword);
        } catch (IllegalArgumentException ex) {
            throw new ServletException("Geoserver manager count not be built", ex);
        } catch (MalformedURLException ex) {
            // This should not happen since we take care of it above - we can probably move this into the try block above
            throw new ServletException("Geoserver endpoint (" + geoserverEndpoint + ") could not be parsed into a valid URL.");
        }

        String dwInitParam = servletConfig.getInitParameter("geoserver-default-workspace");
        String dwJndiProp = props.getProperty("geoserver-default-workspace");
        if (StringUtils.isNotBlank(dwInitParam)) {
            defaultWorkspaceName = dwInitParam;
        } else if (StringUtils.isNotBlank(dwJndiProp)) {
            defaultWorkspaceName = dwInitParam;
        } else {
            defaultWorkspaceName = "";
            LOG.warn("Default workspace is not defined. If a workspace is not passed to during the request, the request will fail;");
        }
        LOG.trace("Default workspace set to: " + defaultWorkspaceName);

        String dsnInitParam = servletConfig.getInitParameter("geoserver-default-storename");
        String dsnJndiProp = props.getProperty("geoserver-default-storename");
        if (StringUtils.isNotBlank(dsnInitParam)) {
            defaultStoreName = dwInitParam;
        } else if (StringUtils.isNotBlank(dsnJndiProp)) {
            defaultStoreName = dwInitParam;
        } else {
            defaultStoreName = "";
            LOG.warn("Default store name is not defined. If a store name is not passed to during the request, the request will fail;");
        }
        LOG.trace("Default store name set to: " + defaultStoreName);

        String dsrsInitParam = servletConfig.getInitParameter("geoserver-default-srs");
        String dsrsJndiProp = props.getProperty("geoserver-default-srs");
        if (StringUtils.isNotBlank(dsrsInitParam)) {
            defaultSRS = dsrsInitParam;
        } else if (StringUtils.isNotBlank(dsrsJndiProp)) {
            defaultSRS = dsrsJndiProp;
        } else {
            defaultSRS = "";
            LOG.warn("Default SRS is not defined. If a SRS name is not passed to during the request, the request will fail;");
        }
        LOG.trace("Default SRS set to: " + defaultSRS);
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, FileNotFoundException {
        doPost(request, response);
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, FileNotFoundException {
        Map<String, String> responseMap = new HashMap<String, String>();

        RequestResponseHelper.ResponseType responseType = RequestResponseHelper.ResponseType.XML;
        String responseEncoding = request.getParameter("response-encoding");
        if (StringUtils.isBlank(responseEncoding) || responseEncoding.toLowerCase().contains("json")) {
            responseType = RequestResponseHelper.ResponseType.JSON;
        }
        LOG.trace("Response type set to " + responseType.toString());

        int fileSize = Integer.parseInt(request.getHeader("Content-Length"));
        if (fileSize > maxFileSize) {
            responseMap.put("error", "Upload exceeds max file size of " + maxFileSize + " bytes");
            RequestResponseHelper.sendErrorResponse(response, responseMap, responseType);
            return;
        }

        if (!StringUtils.isBlank(request.getParameter("filename-param"))) {
            filenameParam = request.getParameter("filename-param");
            LOG.trace("Based on incoming request, filename parameter re-set to: " + filenameParam);
        }

        String filename = request.getParameter(filenameParam);
        String tempDir = System.getProperty("java.io.tmpdir");
        File shapeZipFile = new File(tempDir + File.separator + filename);
        LOG.trace("Temporary file set to " + shapeZipFile.getPath());

        String workspaceName = request.getParameter("workspace");
        if (StringUtils.isBlank(workspaceName)) {
            workspaceName = defaultWorkspaceName;
        }
        if (StringUtils.isBlank(workspaceName)) {
            responseMap.put("error", "Parameter \"workspace\" is mandatory");
            RequestResponseHelper.sendErrorResponse(response, responseMap, responseType);
            return;
        }
        LOG.trace("Workspace name set to " + workspaceName);

        String storeName = request.getParameter("store");
        if (StringUtils.isBlank(storeName)) {
            storeName = defaultStoreName;
        }
        if (StringUtils.isBlank(storeName)) {
            responseMap.put("error", "Parameter \"store\" is mandatory");
            RequestResponseHelper.sendErrorResponse(response, responseMap, responseType);
            return;
        }
        LOG.trace("Store name set to " + storeName);

        String srsName = request.getParameter("srs");
        if (StringUtils.isBlank(srsName)) {
            srsName = defaultSRS;
        }
        if (StringUtils.isBlank(srsName)) {
            responseMap.put("error", "Parameter \"srs\" is mandatory");
            RequestResponseHelper.sendErrorResponse(response, responseMap, responseType);
            return;
        }
        LOG.trace("SRS name set to " + srsName);
        
        String layerName = request.getParameter("layer");
        if (StringUtils.isBlank(layerName)) {
            layerName = filename.split(".")[0];
        }
        layerName = layerName.trim().replaceAll("\\.", "_").replaceAll(" ", "_");
        LOG.trace("Layer name set to " + layerName);
        
        try {
            RequestResponseHelper.saveFileFromRequest(request, shapeZipFile, filenameParam);
            LOG.trace("File saved to " + shapeZipFile.getPath());

            FileHelper.flattenZipFile(shapeZipFile.getPath());
            LOG.trace("Zip file directory structure flattened");

            if (!FileHelper.validateShapefileZip(shapeZipFile)) {
                throw new IOException("Unable to verify shapefile. Upload failed.");
            }
            LOG.trace("Zip file seems to be a valid shapefile");
        } catch (Exception ex) {
            LOG.warn(ex.getMessage());
            responseMap.put("error", "Unable to upload file");
            responseMap.put("exception", ex.getMessage());
            RequestResponseHelper.sendErrorResponse(response, responseMap, responseType);
            return;
        }

        try {
            GeoServerRESTPublisher gsPublisher = gsRestManager.getPublisher();
            
//            // Do EPSG processing
//        String declaredCRS = null;
//        String nativeCRS = null;
//        String warning = "";
//        try {
//            nativeCRS = new String(FileHelper.getByteArrayFromFile(new File(renamedPrjPath)));
//            if (nativeCRS == null || nativeCRS.isEmpty()) {
//                String errorMessage = "Error while getting Prj/WKT information from PRJ file. Function halted.";
//                LOGGER.error(errorMessage);
//                addError(errorMessage);
//                throw new RuntimeException(errorMessage);
//            }
//            // The behavior of this method requires that the layer always force
//            // projection from native to declared...
//            declaredCRS = ShapeFileEPSGHelper.getDeclaredEPSGFromWKT(nativeCRS, false);
//            if (declaredCRS == null || declaredCRS.isEmpty()) {
//                declaredCRS = ShapeFileEPSGHelper.getDeclaredEPSGFromWKT(nativeCRS, true);
//                warning = "Could not find EPSG code for prj definition. The geographic coordinate system '" + declaredCRS + "' will be used";
//            } else if (declaredCRS.startsWith("ESRI:")) {
//                declaredCRS = declaredCRS.replaceFirst("ESRI:", "EPSG:");
//            }
//            if (declaredCRS == null || declaredCRS.isEmpty()) {
//                throw new RuntimeException("Could not attain EPSG code from shapefile. Please ensure proper projection and a valid PRJ file.");
//            }
//        } catch (Exception ex) {
//            String errorMessage = "Error while getting EPSG information from PRJ file. Function halted.";
//            LOGGER.error(errorMessage, ex);
//            addError(errorMessage);
//            throw new RuntimeException(errorMessage, ex);
//        }
            
            // Currently not doing any checks to see if workspace, store or layer already exist
            Boolean success = gsPublisher.publishShp(workspaceName, storeName, null, layerName, UploadMethod.FILE, shapeZipFile.toURI(), srsName, "nativeSRS", ProjectionPolicy.NONE, null);
            
            if (success) {
                LOG.debug("Shapefile has been imported successfully");
                RequestResponseHelper.sendSuccessResponse(response, responseMap, responseType);
            } else {
                LOG.debug("Shapefile could not be imported successfully");
                responseMap.put("error", "Shapefile could not be imported successfully");
                RequestResponseHelper.sendErrorResponse(response, responseMap, responseType);
            }
        } catch (Exception ex) {
            LOG.warn(ex.getMessage());
            responseMap.put("error", "Unable to upload file");
            responseMap.put("exception", ex.getMessage());
            RequestResponseHelper.sendErrorResponse(response, responseMap, responseType);
        } finally {
            FileUtils.deleteQuietly(shapeZipFile);
        }
    }
}

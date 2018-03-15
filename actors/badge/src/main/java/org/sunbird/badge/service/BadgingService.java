package org.sunbird.badge.service;

import java.io.IOException;

import org.sunbird.common.models.response.Response;
import org.sunbird.common.request.Request;

/**
 * This interface will have all the methods required for badging framework. any
 * new method required for badging need to be added here.
 * 
 * @author Manzarul
 *
 */
public interface BadgingService {

	/**
	 * This method will create an issuer based on user requested data.
	 * 
	 * @param request
	 *            Request
	 * @exception IOException
	 * @return Response
	 */
	public Response createIssuer(Request request) throws IOException;

	/**
	 * This method will provide particular issuer details based on requested issuer
	 * id.
	 * 
	 * @param request
	 *            Request
	 * @exception IOException
	 * @return Response
	 */
	public Response getIssuerDetails(Request request) throws IOException;

	/**
	 * This method will provide list of issuers.
	 * 
	 * @param request
	 *            Request
	 * @exception IOException
	 * @return Response
	 */
	public Response getIssuerList(Request request) throws IOException;

	/**
	 * This method will delete issuer from server. the issuer can be deleted if they
	 * haven't issued any badge.
	 * 
	 * @param request
	 *            Request
	 * @exception IOException
	 * @return Response
	 */
	public Response removeIssuer(Request request) throws IOException;

	/**
	 * This method will create a badge class for a particular issuer.
	 * 
	 * @param request
	 *            Request
	 * @exception IOException
	 * @return Response
	 */
	public Response createBadgeClass(Request request) throws IOException;

	/**
	 * This method will provide badge class details based on issuerId and badgeId
	 * 
	 * @param request
	 *            Request
	 * @exception IOException
	 * @return Response
	 */
	public Response getBadgeClassDetails(Request request) throws IOException;

	/**
	 * This method will provide list of badge class either for one issuer or
	 * multiple or all , this will depends on requested param.
	 * 
	 * @param request
	 *            Request
	 * @exception IOException
	 * @return Response
	 */
	public Response getBadgeClassList(Request request) throws IOException;

	/**
	 * This method will remove badge class from db. Badge class can be removed if it
	 * is not assign to any one.
	 * 
	 * @param request
	 *            Request
	 * @exception IOException
	 * @return Response
	 */
	public Response removeBadgeClass(Request request) throws IOException;

	/**
	 * This method will issue a badge assertion to either user or content.
	 * 
	 * @param request
	 *            Request
	 * @exception IOException
	 * @return Response
	 */
	public Response badgeAssertion(Request request) throws IOException;

	/**
	 * This method will provide particular assertion details.
	 * 
	 * @param request
	 *            Request
	 * @exception IOException
	 * @return Response
	 */
	public Response getAssertionDetails(Request request) throws IOException;

	/**
	 * This method will provide list of assertions.
	 * 
	 * @param request
	 *            Request
	 * @exception IOException
	 * @return Response
	 */
	public Response getAssertionList(Request request) throws IOException;

	/**
	 * This method is used to remove the assertion from either user or content.
	 * 
	 * @param request
	 *            Request
	 * @exception IOException
	 * @return Response
	 */
	public Response revokeAssertion(Request request) throws IOException;

	/**
	 * This method will provide particular issuer details based on requested issuer
	 * id.
	 *
	 * @param request
	 *            Request
	 * @exception IOException
	 * @return Response
	 */
	public Response deleteIssuer(Request request) throws IOException;
}

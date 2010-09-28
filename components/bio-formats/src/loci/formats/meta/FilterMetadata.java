
/*
 * loci.formats.meta.FilterMetadata
 *
 *-----------------------------------------------------------------------------
 *
 *  Copyright (C) 2005-@year@ Open Microscopy Environment
 *      Massachusetts Institute of Technology,
 *      National Institutes of Health,
 *      University of Dundee,
 *      University of Wisconsin-Madison
 *
 *
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation; either
 *    version 2.1 of the License, or (at your option) any later version.
 *
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *    Lesser General Public License for more details.
 *
 *    You should have received a copy of the GNU Lesser General Public
 *    License along with this library; if not, write to the Free Software
 *    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 *-----------------------------------------------------------------------------
 */

/*-----------------------------------------------------------------------------
 *
 * THIS IS AUTOMATICALLY GENERATED CODE.  DO NOT MODIFY.
 * Created by callan via xsd-fu on 2010-07-01 12:04:40+0100
 *
 *-----------------------------------------------------------------------------
 */

package loci.formats.meta;

import loci.common.DataTools;

import ome.xml.model.enums.*;
import ome.xml.model.primitives.*;

/**
 * An implementation of {@link MetadataStore} that removes unprintable
 * characters from metadata values before storing them in a delegate
 * MetadataStore.
 *
 * <dl><dt><b>Source code:</b></dt>
 * <dd><a href="http://dev.loci.wisc.edu/trac/java/browser/trunk/components/bio-formats/src/loci/formats/meta/FilterMetadata.java">Trac</a>,
 * <a href="http://dev.loci.wisc.edu/svn/java/trunk/components/bio-formats/src/loci/formats/meta/FilterMetadata.java">SVN</a></dd></dl>
 *
 * @author Melissa Linkert melissa at glencoesoftware.com
 * @author Curtis Rueden ctrueden at wisc.edu
 */
public class FilterMetadata implements MetadataStore
{
	// -- Fields --

	private MetadataStore store;
	private boolean filter;

	// -- Constructor --

	public FilterMetadata(MetadataStore store, boolean filter)
	{
		this.store = store;
		this.filter = filter;
	}

	// -- MetadataStore API methods --

	/* @see MetadataStore#createRoot() */
	public void createRoot()
	{
		store.createRoot();
	}

	/* @see MetadataStore#getRoot() */
	public Object getRoot()
	{
		return store.getRoot();
	}

	/* @see MetadataStore#setRoot(Object) */
	public void setRoot(Object root)
	{
		store.setRoot(root);
	}

	/* @see MetadataStore#setUUID(String) */
	public void setUUID(String uuid)
	{
		store.setUUID(uuid);
	}

	// -- AggregateMetadata API methods --

	// -- Entity storage (manual definitions) --

	public void setPixelsBinDataBigEndian(Boolean bigEndian, int imageIndex, int binDataIndex)
	{
		store.setPixelsBinDataBigEndian(bigEndian, imageIndex, binDataIndex);
	}

	public void setMaskBinData(byte[] binData, int ROIIndex, int shapeIndex)
	{
		store.setMaskBinData(binData, ROIIndex, shapeIndex);
	}

	// -- Entity storage (code generated definitions) --

	//
	// AnnotationRef property storage
	//
	// {u'ROI': {u'OME': None}, u'PlateAcquisition': {u'Plate': {u'OME': None}}, u'Plate': {u'OME': None}, u'Image': {u'OME': None}, u'Screen': {u'OME': None}, u'Well': {u'Plate': {u'OME': None}}, u'Dataset': {u'OME': None}, u'Project': {u'OME': None}, u'Reagent': {u'Screen': {u'OME': None}}, u'Plane': {u'Pixels': {u'Image': {u'OME': None}}}, u'Experimenter': {u'OME': None}, u'Annotation': None, u'WellSample': {u'Well': {u'Plate': {u'OME': None}}}, u'Pixels': {u'Image': {u'OME': None}}, u'Channel': {u'Pixels': {u'Image': {u'OME': None}}}}
	// Is multi path? True

	// 1:1
	// Is multi path? True
	// Ignoring ID property of reference AnnotationRef

	//
	// Arc property storage
	//
	// {u'LightSource': {u'Instrument': {u'OME': None}}}
	// Is multi path? False

	// Ignoring Arc of parent abstract type
	// Ignoring Filament of parent abstract type
	// ID accessor from parent LightSource
	public void setArcID(String id, int instrumentIndex, int lightSourceIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setArcID(id, instrumentIndex, lightSourceIndex);
	}

	// Ignoring Laser of parent abstract type
	// Ignoring LightEmittingDiode of parent abstract type
	// LotNumber accessor from parent LightSource
	public void setArcLotNumber(String lotNumber, int instrumentIndex, int lightSourceIndex)
	{
		lotNumber = filter? DataTools.sanitize(lotNumber) : lotNumber;
		store.setArcLotNumber(lotNumber, instrumentIndex, lightSourceIndex);
	}

	// Manufacturer accessor from parent LightSource
	public void setArcManufacturer(String manufacturer, int instrumentIndex, int lightSourceIndex)
	{
		manufacturer = filter? DataTools.sanitize(manufacturer) : manufacturer;
		store.setArcManufacturer(manufacturer, instrumentIndex, lightSourceIndex);
	}

	// Model accessor from parent LightSource
	public void setArcModel(String model, int instrumentIndex, int lightSourceIndex)
	{
		model = filter? DataTools.sanitize(model) : model;
		store.setArcModel(model, instrumentIndex, lightSourceIndex);
	}

	// Power accessor from parent LightSource
	public void setArcPower(Double power, int instrumentIndex, int lightSourceIndex)
	{
		store.setArcPower(power, instrumentIndex, lightSourceIndex);
	}

	// SerialNumber accessor from parent LightSource
	public void setArcSerialNumber(String serialNumber, int instrumentIndex, int lightSourceIndex)
	{
		serialNumber = filter? DataTools.sanitize(serialNumber) : serialNumber;
		store.setArcSerialNumber(serialNumber, instrumentIndex, lightSourceIndex);
	}

	public void setArcType(ArcType type, int instrumentIndex, int lightSourceIndex)
	{
		store.setArcType(type, instrumentIndex, lightSourceIndex);
	}

	//
	// BinaryFile property storage
	//
	// {u'FileAnnotation': {u'StructuredAnnotations': {u'OME': None}}, u'OTF': {u'Instrument': {u'OME': None}}}
	// Is multi path? True

	// Ignoring BinData element, complex property
	// Ignoring External element, complex property
	public void setFileAnnotationBinaryFileFileName(String fileName, int fileAnnotationIndex)
	{
		fileName = filter? DataTools.sanitize(fileName) : fileName;
		store.setFileAnnotationBinaryFileFileName(fileName, fileAnnotationIndex);
	}

	public void setOTFBinaryFileFileName(String fileName, int instrumentIndex, int OTFIndex)
	{
		fileName = filter? DataTools.sanitize(fileName) : fileName;
		store.setOTFBinaryFileFileName(fileName, instrumentIndex, OTFIndex);
	}

	public void setFileAnnotationBinaryFileMIMEType(String mimetype, int fileAnnotationIndex)
	{
		mimetype = filter? DataTools.sanitize(mimetype) : mimetype;
		store.setFileAnnotationBinaryFileMIMEType(mimetype, fileAnnotationIndex);
	}

	public void setOTFBinaryFileMIMEType(String mimetype, int instrumentIndex, int OTFIndex)
	{
		mimetype = filter? DataTools.sanitize(mimetype) : mimetype;
		store.setOTFBinaryFileMIMEType(mimetype, instrumentIndex, OTFIndex);
	}

	public void setFileAnnotationBinaryFileSize(NonNegativeLong size, int fileAnnotationIndex)
	{
		store.setFileAnnotationBinaryFileSize(size, fileAnnotationIndex);
	}

	public void setOTFBinaryFileSize(NonNegativeLong size, int instrumentIndex, int OTFIndex)
	{
		store.setOTFBinaryFileSize(size, instrumentIndex, OTFIndex);
	}

	//
	// BooleanAnnotation property storage
	//
	// {u'StructuredAnnotations': {u'OME': None}}
	// Is multi path? False

	public void setBooleanAnnotationAnnotationRef(String annotation, int booleanAnnotationIndex, int annotationRefIndex)
	{
		store.setBooleanAnnotationAnnotationRef(annotation, booleanAnnotationIndex, annotationRefIndex);
	}

	// Ignoring Channel_BackReference back reference
	// Ignoring Dataset_BackReference back reference
	public void setBooleanAnnotationDescription(String description, int booleanAnnotationIndex)
	{
		description = filter? DataTools.sanitize(description) : description;
		store.setBooleanAnnotationDescription(description, booleanAnnotationIndex);
	}

	// Ignoring Experimenter_BackReference back reference
	public void setBooleanAnnotationID(String id, int booleanAnnotationIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setBooleanAnnotationID(id, booleanAnnotationIndex);
	}

	// Ignoring Image_BackReference back reference
	public void setBooleanAnnotationNamespace(String namespace, int booleanAnnotationIndex)
	{
		namespace = filter? DataTools.sanitize(namespace) : namespace;
		store.setBooleanAnnotationNamespace(namespace, booleanAnnotationIndex);
	}

	// Ignoring Pixels_BackReference back reference
	// Ignoring Plane_BackReference back reference
	// Ignoring PlateAcquisition_BackReference back reference
	// Ignoring Plate_BackReference back reference
	// Ignoring Project_BackReference back reference
	// Ignoring ROI_BackReference back reference
	// Ignoring Reagent_BackReference back reference
	// Ignoring Screen_BackReference back reference
	public void setBooleanAnnotationValue(Boolean value, int booleanAnnotationIndex)
	{
		store.setBooleanAnnotationValue(value, booleanAnnotationIndex);
	}

	// Ignoring WellSample_BackReference back reference
	// Ignoring Well_BackReference back reference
	//
	// Channel property storage
	//
	// {u'Pixels': {u'Image': {u'OME': None}}}
	// Is multi path? False

	public void setChannelAcquisitionMode(AcquisitionMode acquisitionMode, int imageIndex, int channelIndex)
	{
		store.setChannelAcquisitionMode(acquisitionMode, imageIndex, channelIndex);
	}

	public void setChannelAnnotationRef(String annotation, int imageIndex, int channelIndex, int annotationRefIndex)
	{
		store.setChannelAnnotationRef(annotation, imageIndex, channelIndex, annotationRefIndex);
	}

	public void setChannelColor(Integer color, int imageIndex, int channelIndex)
	{
		store.setChannelColor(color, imageIndex, channelIndex);
	}

	public void setChannelContrastMethod(ContrastMethod contrastMethod, int imageIndex, int channelIndex)
	{
		store.setChannelContrastMethod(contrastMethod, imageIndex, channelIndex);
	}

	// Ignoring DetectorSettings element, complex property
	public void setChannelEmissionWavelength(PositiveInteger emissionWavelength, int imageIndex, int channelIndex)
	{
		store.setChannelEmissionWavelength(emissionWavelength, imageIndex, channelIndex);
	}

	public void setChannelExcitationWavelength(PositiveInteger excitationWavelength, int imageIndex, int channelIndex)
	{
		store.setChannelExcitationWavelength(excitationWavelength, imageIndex, channelIndex);
	}

	public void setChannelFilterSetRef(String filterSet, int imageIndex, int channelIndex)
	{
		store.setChannelFilterSetRef(filterSet, imageIndex, channelIndex);
	}

	public void setChannelFluor(String fluor, int imageIndex, int channelIndex)
	{
		fluor = filter? DataTools.sanitize(fluor) : fluor;
		store.setChannelFluor(fluor, imageIndex, channelIndex);
	}

	public void setChannelID(String id, int imageIndex, int channelIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setChannelID(id, imageIndex, channelIndex);
	}

	public void setChannelIlluminationType(IlluminationType illuminationType, int imageIndex, int channelIndex)
	{
		store.setChannelIlluminationType(illuminationType, imageIndex, channelIndex);
	}

	// Ignoring LightPath element, complex property
	// Ignoring LightSourceSettings element, complex property
	public void setChannelNDFilter(Double ndfilter, int imageIndex, int channelIndex)
	{
		store.setChannelNDFilter(ndfilter, imageIndex, channelIndex);
	}

	public void setChannelName(String name, int imageIndex, int channelIndex)
	{
		name = filter? DataTools.sanitize(name) : name;
		store.setChannelName(name, imageIndex, channelIndex);
	}

	public void setChannelOTFRef(String otf, int imageIndex, int channelIndex)
	{
		store.setChannelOTFRef(otf, imageIndex, channelIndex);
	}

	public void setChannelPinholeSize(Double pinholeSize, int imageIndex, int channelIndex)
	{
		store.setChannelPinholeSize(pinholeSize, imageIndex, channelIndex);
	}

	public void setChannelPockelCellSetting(Integer pockelCellSetting, int imageIndex, int channelIndex)
	{
		store.setChannelPockelCellSetting(pockelCellSetting, imageIndex, channelIndex);
	}

	public void setChannelSamplesPerPixel(PositiveInteger samplesPerPixel, int imageIndex, int channelIndex)
	{
		store.setChannelSamplesPerPixel(samplesPerPixel, imageIndex, channelIndex);
	}

	//
	// CommentAnnotation property storage
	//
	// {u'StructuredAnnotations': {u'OME': None}}
	// Is multi path? False

	public void setCommentAnnotationAnnotationRef(String annotation, int commentAnnotationIndex, int annotationRefIndex)
	{
		store.setCommentAnnotationAnnotationRef(annotation, commentAnnotationIndex, annotationRefIndex);
	}

	// Ignoring Channel_BackReference back reference
	// Ignoring Dataset_BackReference back reference
	public void setCommentAnnotationDescription(String description, int commentAnnotationIndex)
	{
		description = filter? DataTools.sanitize(description) : description;
		store.setCommentAnnotationDescription(description, commentAnnotationIndex);
	}

	// Ignoring Experimenter_BackReference back reference
	public void setCommentAnnotationID(String id, int commentAnnotationIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setCommentAnnotationID(id, commentAnnotationIndex);
	}

	// Ignoring Image_BackReference back reference
	public void setCommentAnnotationNamespace(String namespace, int commentAnnotationIndex)
	{
		namespace = filter? DataTools.sanitize(namespace) : namespace;
		store.setCommentAnnotationNamespace(namespace, commentAnnotationIndex);
	}

	// Ignoring Pixels_BackReference back reference
	// Ignoring Plane_BackReference back reference
	// Ignoring PlateAcquisition_BackReference back reference
	// Ignoring Plate_BackReference back reference
	// Ignoring Project_BackReference back reference
	// Ignoring ROI_BackReference back reference
	// Ignoring Reagent_BackReference back reference
	// Ignoring Screen_BackReference back reference
	public void setCommentAnnotationValue(String value, int commentAnnotationIndex)
	{
		value = filter? DataTools.sanitize(value) : value;
		store.setCommentAnnotationValue(value, commentAnnotationIndex);
	}

	// Ignoring WellSample_BackReference back reference
	// Ignoring Well_BackReference back reference
	//
	// Contact property storage
	//
	// {u'Group': {u'OME': None}}
	// Is multi path? False

	// 1:1
	// Is multi path? False
	// Ignoring ID property of reference Contact

	//
	// Dataset property storage
	//
	// {u'OME': None}
	// Is multi path? False

	public void setDatasetAnnotationRef(String annotation, int datasetIndex, int annotationRefIndex)
	{
		store.setDatasetAnnotationRef(annotation, datasetIndex, annotationRefIndex);
	}

	public void setDatasetDescription(String description, int datasetIndex)
	{
		description = filter? DataTools.sanitize(description) : description;
		store.setDatasetDescription(description, datasetIndex);
	}

	public void setDatasetExperimenterRef(String experimenter, int datasetIndex)
	{
		store.setDatasetExperimenterRef(experimenter, datasetIndex);
	}

	public void setDatasetGroupRef(String group, int datasetIndex)
	{
		store.setDatasetGroupRef(group, datasetIndex);
	}

	public void setDatasetID(String id, int datasetIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setDatasetID(id, datasetIndex);
	}

	// Ignoring Image_BackReference back reference
	public void setDatasetName(String name, int datasetIndex)
	{
		name = filter? DataTools.sanitize(name) : name;
		store.setDatasetName(name, datasetIndex);
	}

	public void setDatasetProjectRef(String project, int datasetIndex, int projectRefIndex)
	{
		store.setDatasetProjectRef(project, datasetIndex, projectRefIndex);
	}

	//
	// DatasetRef property storage
	//
	// {u'Image': {u'OME': None}}
	// Is multi path? False

	// 1:1
	// Is multi path? False
	// Ignoring ID property of reference DatasetRef

	//
	// Detector property storage
	//
	// {u'Instrument': {u'OME': None}}
	// Is multi path? False

	public void setDetectorAmplificationGain(Double amplificationGain, int instrumentIndex, int detectorIndex)
	{
		store.setDetectorAmplificationGain(amplificationGain, instrumentIndex, detectorIndex);
	}

	public void setDetectorGain(Double gain, int instrumentIndex, int detectorIndex)
	{
		store.setDetectorGain(gain, instrumentIndex, detectorIndex);
	}

	public void setDetectorID(String id, int instrumentIndex, int detectorIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setDetectorID(id, instrumentIndex, detectorIndex);
	}

	public void setDetectorLotNumber(String lotNumber, int instrumentIndex, int detectorIndex)
	{
		lotNumber = filter? DataTools.sanitize(lotNumber) : lotNumber;
		store.setDetectorLotNumber(lotNumber, instrumentIndex, detectorIndex);
	}

	public void setDetectorManufacturer(String manufacturer, int instrumentIndex, int detectorIndex)
	{
		manufacturer = filter? DataTools.sanitize(manufacturer) : manufacturer;
		store.setDetectorManufacturer(manufacturer, instrumentIndex, detectorIndex);
	}

	public void setDetectorModel(String model, int instrumentIndex, int detectorIndex)
	{
		model = filter? DataTools.sanitize(model) : model;
		store.setDetectorModel(model, instrumentIndex, detectorIndex);
	}

	public void setDetectorOffset(Double offset, int instrumentIndex, int detectorIndex)
	{
		store.setDetectorOffset(offset, instrumentIndex, detectorIndex);
	}

	public void setDetectorSerialNumber(String serialNumber, int instrumentIndex, int detectorIndex)
	{
		serialNumber = filter? DataTools.sanitize(serialNumber) : serialNumber;
		store.setDetectorSerialNumber(serialNumber, instrumentIndex, detectorIndex);
	}

	public void setDetectorType(DetectorType type, int instrumentIndex, int detectorIndex)
	{
		store.setDetectorType(type, instrumentIndex, detectorIndex);
	}

	public void setDetectorVoltage(Double voltage, int instrumentIndex, int detectorIndex)
	{
		store.setDetectorVoltage(voltage, instrumentIndex, detectorIndex);
	}

	public void setDetectorZoom(Double zoom, int instrumentIndex, int detectorIndex)
	{
		store.setDetectorZoom(zoom, instrumentIndex, detectorIndex);
	}

	//
	// DetectorSettings property storage
	//
	// {u'Channel': {u'Pixels': {u'Image': {u'OME': None}}}}
	// Is multi path? False

	public void setDetectorSettingsBinning(Binning binning, int imageIndex, int channelIndex)
	{
		store.setDetectorSettingsBinning(binning, imageIndex, channelIndex);
	}

	public void setDetectorSettingsGain(Double gain, int imageIndex, int channelIndex)
	{
		store.setDetectorSettingsGain(gain, imageIndex, channelIndex);
	}

	public void setDetectorSettingsID(String id, int imageIndex, int channelIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setDetectorSettingsID(id, imageIndex, channelIndex);
	}

	public void setDetectorSettingsOffset(Double offset, int imageIndex, int channelIndex)
	{
		store.setDetectorSettingsOffset(offset, imageIndex, channelIndex);
	}

	public void setDetectorSettingsReadOutRate(Double readOutRate, int imageIndex, int channelIndex)
	{
		store.setDetectorSettingsReadOutRate(readOutRate, imageIndex, channelIndex);
	}

	public void setDetectorSettingsVoltage(Double voltage, int imageIndex, int channelIndex)
	{
		store.setDetectorSettingsVoltage(voltage, imageIndex, channelIndex);
	}

	//
	// Dichroic property storage
	//
	// {u'Instrument': {u'OME': None}}
	// Is multi path? False

	// Ignoring FilterSet_BackReference back reference
	public void setDichroicID(String id, int instrumentIndex, int dichroicIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setDichroicID(id, instrumentIndex, dichroicIndex);
	}

	// Ignoring LightPath_BackReference back reference
	public void setDichroicLotNumber(String lotNumber, int instrumentIndex, int dichroicIndex)
	{
		lotNumber = filter? DataTools.sanitize(lotNumber) : lotNumber;
		store.setDichroicLotNumber(lotNumber, instrumentIndex, dichroicIndex);
	}

	public void setDichroicManufacturer(String manufacturer, int instrumentIndex, int dichroicIndex)
	{
		manufacturer = filter? DataTools.sanitize(manufacturer) : manufacturer;
		store.setDichroicManufacturer(manufacturer, instrumentIndex, dichroicIndex);
	}

	public void setDichroicModel(String model, int instrumentIndex, int dichroicIndex)
	{
		model = filter? DataTools.sanitize(model) : model;
		store.setDichroicModel(model, instrumentIndex, dichroicIndex);
	}

	public void setDichroicSerialNumber(String serialNumber, int instrumentIndex, int dichroicIndex)
	{
		serialNumber = filter? DataTools.sanitize(serialNumber) : serialNumber;
		store.setDichroicSerialNumber(serialNumber, instrumentIndex, dichroicIndex);
	}

	//
	// DichroicRef property storage
	//
	// {u'LightPath': {u'Channel': {u'Pixels': {u'Image': {u'OME': None}}}}, u'FilterSet': {u'Instrument': {u'OME': None}}}
	// Is multi path? True

	// 1:1
	// Is multi path? True
	// Ignoring ID property of reference DichroicRef

	//
	// DoubleAnnotation property storage
	//
	// {u'StructuredAnnotations': {u'OME': None}}
	// Is multi path? False

	public void setDoubleAnnotationAnnotationRef(String annotation, int doubleAnnotationIndex, int annotationRefIndex)
	{
		store.setDoubleAnnotationAnnotationRef(annotation, doubleAnnotationIndex, annotationRefIndex);
	}

	// Ignoring Channel_BackReference back reference
	// Ignoring Dataset_BackReference back reference
	public void setDoubleAnnotationDescription(String description, int doubleAnnotationIndex)
	{
		description = filter? DataTools.sanitize(description) : description;
		store.setDoubleAnnotationDescription(description, doubleAnnotationIndex);
	}

	// Ignoring Experimenter_BackReference back reference
	public void setDoubleAnnotationID(String id, int doubleAnnotationIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setDoubleAnnotationID(id, doubleAnnotationIndex);
	}

	// Ignoring Image_BackReference back reference
	public void setDoubleAnnotationNamespace(String namespace, int doubleAnnotationIndex)
	{
		namespace = filter? DataTools.sanitize(namespace) : namespace;
		store.setDoubleAnnotationNamespace(namespace, doubleAnnotationIndex);
	}

	// Ignoring Pixels_BackReference back reference
	// Ignoring Plane_BackReference back reference
	// Ignoring PlateAcquisition_BackReference back reference
	// Ignoring Plate_BackReference back reference
	// Ignoring Project_BackReference back reference
	// Ignoring ROI_BackReference back reference
	// Ignoring Reagent_BackReference back reference
	// Ignoring Screen_BackReference back reference
	public void setDoubleAnnotationValue(Double value, int doubleAnnotationIndex)
	{
		store.setDoubleAnnotationValue(value, doubleAnnotationIndex);
	}

	// Ignoring WellSample_BackReference back reference
	// Ignoring Well_BackReference back reference
	//
	// Ellipse property storage
	//
	// {u'Shape': {u'Union': {u'ROI': {u'OME': None}}}}
	// Is multi path? False

	// Description accessor from parent Shape
	public void setEllipseDescription(String description, int ROIIndex, int shapeIndex)
	{
		description = filter? DataTools.sanitize(description) : description;
		store.setEllipseDescription(description, ROIIndex, shapeIndex);
	}

	// Ignoring Ellipse of parent abstract type
	// Fill accessor from parent Shape
	public void setEllipseFill(Integer fill, int ROIIndex, int shapeIndex)
	{
		store.setEllipseFill(fill, ROIIndex, shapeIndex);
	}

	// Ignoring FillRule of parent abstract type
	// Ignoring FontFamily of parent abstract type
	// FontSize accessor from parent Shape
	public void setEllipseFontSize(NonNegativeInteger fontSize, int ROIIndex, int shapeIndex)
	{
		store.setEllipseFontSize(fontSize, ROIIndex, shapeIndex);
	}

	// Ignoring FontStyle of parent abstract type
	// ID accessor from parent Shape
	public void setEllipseID(String id, int ROIIndex, int shapeIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setEllipseID(id, ROIIndex, shapeIndex);
	}

	// Label accessor from parent Shape
	public void setEllipseLabel(String label, int ROIIndex, int shapeIndex)
	{
		label = filter? DataTools.sanitize(label) : label;
		store.setEllipseLabel(label, ROIIndex, shapeIndex);
	}

	// Ignoring Line of parent abstract type
	// Ignoring LineCap of parent abstract type
	// Ignoring MarkerEnd of parent abstract type
	// Ignoring MarkerStart of parent abstract type
	// Ignoring Mask of parent abstract type
	// Name accessor from parent Shape
	public void setEllipseName(String name, int ROIIndex, int shapeIndex)
	{
		name = filter? DataTools.sanitize(name) : name;
		store.setEllipseName(name, ROIIndex, shapeIndex);
	}

	// Ignoring Path of parent abstract type
	// Ignoring Point of parent abstract type
	// Ignoring Polyline of parent abstract type
	// Ignoring Rectangle of parent abstract type
	// Stroke accessor from parent Shape
	public void setEllipseStroke(Integer stroke, int ROIIndex, int shapeIndex)
	{
		store.setEllipseStroke(stroke, ROIIndex, shapeIndex);
	}

	// StrokeDashArray accessor from parent Shape
	public void setEllipseStrokeDashArray(String strokeDashArray, int ROIIndex, int shapeIndex)
	{
		strokeDashArray = filter? DataTools.sanitize(strokeDashArray) : strokeDashArray;
		store.setEllipseStrokeDashArray(strokeDashArray, ROIIndex, shapeIndex);
	}

	// StrokeWidth accessor from parent Shape
	public void setEllipseStrokeWidth(Double strokeWidth, int ROIIndex, int shapeIndex)
	{
		store.setEllipseStrokeWidth(strokeWidth, ROIIndex, shapeIndex);
	}

	// Ignoring Text of parent abstract type
	// TheC accessor from parent Shape
	public void setEllipseTheC(NonNegativeInteger theC, int ROIIndex, int shapeIndex)
	{
		store.setEllipseTheC(theC, ROIIndex, shapeIndex);
	}

	// TheT accessor from parent Shape
	public void setEllipseTheT(NonNegativeInteger theT, int ROIIndex, int shapeIndex)
	{
		store.setEllipseTheT(theT, ROIIndex, shapeIndex);
	}

	// TheZ accessor from parent Shape
	public void setEllipseTheZ(NonNegativeInteger theZ, int ROIIndex, int shapeIndex)
	{
		store.setEllipseTheZ(theZ, ROIIndex, shapeIndex);
	}

	// Transform accessor from parent Shape
	public void setEllipseTransform(String transform, int ROIIndex, int shapeIndex)
	{
		transform = filter? DataTools.sanitize(transform) : transform;
		store.setEllipseTransform(transform, ROIIndex, shapeIndex);
	}

	public void setEllipseRadiusX(Double radiusX, int ROIIndex, int shapeIndex)
	{
		store.setEllipseRadiusX(radiusX, ROIIndex, shapeIndex);
	}

	public void setEllipseRadiusY(Double radiusY, int ROIIndex, int shapeIndex)
	{
		store.setEllipseRadiusY(radiusY, ROIIndex, shapeIndex);
	}

	public void setEllipseX(Double x, int ROIIndex, int shapeIndex)
	{
		store.setEllipseX(x, ROIIndex, shapeIndex);
	}

	public void setEllipseY(Double y, int ROIIndex, int shapeIndex)
	{
		store.setEllipseY(y, ROIIndex, shapeIndex);
	}

	//
	// EmissionFilterRef property storage
	//
	// {u'LightPath': {u'Channel': {u'Pixels': {u'Image': {u'OME': None}}}}, u'FilterSet': {u'Instrument': {u'OME': None}}}
	// Is multi path? True

	//
	// ExcitationFilterRef property storage
	//
	// {u'LightPath': {u'Channel': {u'Pixels': {u'Image': {u'OME': None}}}}, u'FilterSet': {u'Instrument': {u'OME': None}}}
	// Is multi path? True

	//
	// Experiment property storage
	//
	// {u'OME': None}
	// Is multi path? False

	public void setExperimentDescription(String description, int experimentIndex)
	{
		description = filter? DataTools.sanitize(description) : description;
		store.setExperimentDescription(description, experimentIndex);
	}

	public void setExperimentExperimenterRef(String experimenter, int experimentIndex)
	{
		store.setExperimentExperimenterRef(experimenter, experimentIndex);
	}

	public void setExperimentID(String id, int experimentIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setExperimentID(id, experimentIndex);
	}

	// Ignoring Image_BackReference back reference
	// Ignoring MicrobeamManipulation element, complex property
	public void setExperimentType(ExperimentType type, int experimentIndex)
	{
		store.setExperimentType(type, experimentIndex);
	}

	//
	// ExperimentRef property storage
	//
	// {u'Image': {u'OME': None}}
	// Is multi path? False

	// 1:1
	// Is multi path? False
	// Ignoring ID property of reference ExperimentRef

	//
	// Experimenter property storage
	//
	// {u'OME': None}
	// Is multi path? False

	public void setExperimenterAnnotationRef(String annotation, int experimenterIndex, int annotationRefIndex)
	{
		store.setExperimenterAnnotationRef(annotation, experimenterIndex, annotationRefIndex);
	}

	// Ignoring Dataset_BackReference back reference
	public void setExperimenterDisplayName(String displayName, int experimenterIndex)
	{
		displayName = filter? DataTools.sanitize(displayName) : displayName;
		store.setExperimenterDisplayName(displayName, experimenterIndex);
	}

	public void setExperimenterEmail(String email, int experimenterIndex)
	{
		email = filter? DataTools.sanitize(email) : email;
		store.setExperimenterEmail(email, experimenterIndex);
	}

	// Ignoring Experiment_BackReference back reference
	public void setExperimenterFirstName(String firstName, int experimenterIndex)
	{
		firstName = filter? DataTools.sanitize(firstName) : firstName;
		store.setExperimenterFirstName(firstName, experimenterIndex);
	}

	public void setExperimenterGroupRef(String group, int experimenterIndex, int groupRefIndex)
	{
		store.setExperimenterGroupRef(group, experimenterIndex, groupRefIndex);
	}

	public void setExperimenterID(String id, int experimenterIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setExperimenterID(id, experimenterIndex);
	}

	// Ignoring Image_BackReference back reference
	public void setExperimenterInstitution(String institution, int experimenterIndex)
	{
		institution = filter? DataTools.sanitize(institution) : institution;
		store.setExperimenterInstitution(institution, experimenterIndex);
	}

	public void setExperimenterLastName(String lastName, int experimenterIndex)
	{
		lastName = filter? DataTools.sanitize(lastName) : lastName;
		store.setExperimenterLastName(lastName, experimenterIndex);
	}

	// Ignoring MicrobeamManipulation_BackReference back reference
	public void setExperimenterMiddleName(String middleName, int experimenterIndex)
	{
		middleName = filter? DataTools.sanitize(middleName) : middleName;
		store.setExperimenterMiddleName(middleName, experimenterIndex);
	}

	// Ignoring Project_BackReference back reference
	public void setExperimenterUserName(String userName, int experimenterIndex)
	{
		userName = filter? DataTools.sanitize(userName) : userName;
		store.setExperimenterUserName(userName, experimenterIndex);
	}

	//
	// ExperimenterRef property storage
	//
	// {u'Project': {u'OME': None}, u'Image': {u'OME': None}, u'Dataset': {u'OME': None}, u'Experiment': {u'OME': None}, u'MicrobeamManipulation': {u'Experiment': {u'OME': None}}}
	// Is multi path? True

	// 1:1
	// Is multi path? True
	// Ignoring ID property of reference ExperimenterRef

	//
	// Filament property storage
	//
	// {u'LightSource': {u'Instrument': {u'OME': None}}}
	// Is multi path? False

	// Ignoring Arc of parent abstract type
	// Ignoring Filament of parent abstract type
	// ID accessor from parent LightSource
	public void setFilamentID(String id, int instrumentIndex, int lightSourceIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setFilamentID(id, instrumentIndex, lightSourceIndex);
	}

	// Ignoring Laser of parent abstract type
	// Ignoring LightEmittingDiode of parent abstract type
	// LotNumber accessor from parent LightSource
	public void setFilamentLotNumber(String lotNumber, int instrumentIndex, int lightSourceIndex)
	{
		lotNumber = filter? DataTools.sanitize(lotNumber) : lotNumber;
		store.setFilamentLotNumber(lotNumber, instrumentIndex, lightSourceIndex);
	}

	// Manufacturer accessor from parent LightSource
	public void setFilamentManufacturer(String manufacturer, int instrumentIndex, int lightSourceIndex)
	{
		manufacturer = filter? DataTools.sanitize(manufacturer) : manufacturer;
		store.setFilamentManufacturer(manufacturer, instrumentIndex, lightSourceIndex);
	}

	// Model accessor from parent LightSource
	public void setFilamentModel(String model, int instrumentIndex, int lightSourceIndex)
	{
		model = filter? DataTools.sanitize(model) : model;
		store.setFilamentModel(model, instrumentIndex, lightSourceIndex);
	}

	// Power accessor from parent LightSource
	public void setFilamentPower(Double power, int instrumentIndex, int lightSourceIndex)
	{
		store.setFilamentPower(power, instrumentIndex, lightSourceIndex);
	}

	// SerialNumber accessor from parent LightSource
	public void setFilamentSerialNumber(String serialNumber, int instrumentIndex, int lightSourceIndex)
	{
		serialNumber = filter? DataTools.sanitize(serialNumber) : serialNumber;
		store.setFilamentSerialNumber(serialNumber, instrumentIndex, lightSourceIndex);
	}

	public void setFilamentType(FilamentType type, int instrumentIndex, int lightSourceIndex)
	{
		store.setFilamentType(type, instrumentIndex, lightSourceIndex);
	}

	//
	// FileAnnotation property storage
	//
	// {u'StructuredAnnotations': {u'OME': None}}
	// Is multi path? False

	public void setFileAnnotationAnnotationRef(String annotation, int fileAnnotationIndex, int annotationRefIndex)
	{
		store.setFileAnnotationAnnotationRef(annotation, fileAnnotationIndex, annotationRefIndex);
	}

	// Ignoring BinaryFile element, complex property
	// Ignoring Channel_BackReference back reference
	// Ignoring Dataset_BackReference back reference
	public void setFileAnnotationDescription(String description, int fileAnnotationIndex)
	{
		description = filter? DataTools.sanitize(description) : description;
		store.setFileAnnotationDescription(description, fileAnnotationIndex);
	}

	// Ignoring Experimenter_BackReference back reference
	public void setFileAnnotationID(String id, int fileAnnotationIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setFileAnnotationID(id, fileAnnotationIndex);
	}

	// Ignoring Image_BackReference back reference
	public void setFileAnnotationNamespace(String namespace, int fileAnnotationIndex)
	{
		namespace = filter? DataTools.sanitize(namespace) : namespace;
		store.setFileAnnotationNamespace(namespace, fileAnnotationIndex);
	}

	// Ignoring Pixels_BackReference back reference
	// Ignoring Plane_BackReference back reference
	// Ignoring PlateAcquisition_BackReference back reference
	// Ignoring Plate_BackReference back reference
	// Ignoring Project_BackReference back reference
	// Ignoring ROI_BackReference back reference
	// Ignoring Reagent_BackReference back reference
	// Ignoring Screen_BackReference back reference
	// Ignoring WellSample_BackReference back reference
	// Ignoring Well_BackReference back reference
	//
	// Filter property storage
	//
	// {u'Instrument': {u'OME': None}}
	// Is multi path? False

	// Ignoring FilterSet_BackReference back reference
	public void setFilterFilterWheel(String filterWheel, int instrumentIndex, int filterIndex)
	{
		filterWheel = filter? DataTools.sanitize(filterWheel) : filterWheel;
		store.setFilterFilterWheel(filterWheel, instrumentIndex, filterIndex);
	}

	public void setFilterID(String id, int instrumentIndex, int filterIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setFilterID(id, instrumentIndex, filterIndex);
	}

	// Ignoring LightPath_BackReference back reference
	public void setFilterLotNumber(String lotNumber, int instrumentIndex, int filterIndex)
	{
		lotNumber = filter? DataTools.sanitize(lotNumber) : lotNumber;
		store.setFilterLotNumber(lotNumber, instrumentIndex, filterIndex);
	}

	public void setFilterManufacturer(String manufacturer, int instrumentIndex, int filterIndex)
	{
		manufacturer = filter? DataTools.sanitize(manufacturer) : manufacturer;
		store.setFilterManufacturer(manufacturer, instrumentIndex, filterIndex);
	}

	public void setFilterModel(String model, int instrumentIndex, int filterIndex)
	{
		model = filter? DataTools.sanitize(model) : model;
		store.setFilterModel(model, instrumentIndex, filterIndex);
	}

	public void setFilterSerialNumber(String serialNumber, int instrumentIndex, int filterIndex)
	{
		serialNumber = filter? DataTools.sanitize(serialNumber) : serialNumber;
		store.setFilterSerialNumber(serialNumber, instrumentIndex, filterIndex);
	}

	// Ignoring TransmittanceRange element, complex property
	public void setFilterType(FilterType type, int instrumentIndex, int filterIndex)
	{
		store.setFilterType(type, instrumentIndex, filterIndex);
	}

	//
	// FilterSet property storage
	//
	// {u'Instrument': {u'OME': None}}
	// Is multi path? False

	// Ignoring Channel_BackReference back reference
	public void setFilterSetDichroicRef(String dichroic, int instrumentIndex, int filterSetIndex)
	{
		store.setFilterSetDichroicRef(dichroic, instrumentIndex, filterSetIndex);
	}

	public void setFilterSetEmissionFilterRef(String emissionFilter, int instrumentIndex, int filterSetIndex, int emissionFilterRefIndex)
	{
		store.setFilterSetEmissionFilterRef(emissionFilter, instrumentIndex, filterSetIndex, emissionFilterRefIndex);
	}

	public void setFilterSetExcitationFilterRef(String excitationFilter, int instrumentIndex, int filterSetIndex, int excitationFilterRefIndex)
	{
		store.setFilterSetExcitationFilterRef(excitationFilter, instrumentIndex, filterSetIndex, excitationFilterRefIndex);
	}

	public void setFilterSetID(String id, int instrumentIndex, int filterSetIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setFilterSetID(id, instrumentIndex, filterSetIndex);
	}

	public void setFilterSetLotNumber(String lotNumber, int instrumentIndex, int filterSetIndex)
	{
		lotNumber = filter? DataTools.sanitize(lotNumber) : lotNumber;
		store.setFilterSetLotNumber(lotNumber, instrumentIndex, filterSetIndex);
	}

	public void setFilterSetManufacturer(String manufacturer, int instrumentIndex, int filterSetIndex)
	{
		manufacturer = filter? DataTools.sanitize(manufacturer) : manufacturer;
		store.setFilterSetManufacturer(manufacturer, instrumentIndex, filterSetIndex);
	}

	public void setFilterSetModel(String model, int instrumentIndex, int filterSetIndex)
	{
		model = filter? DataTools.sanitize(model) : model;
		store.setFilterSetModel(model, instrumentIndex, filterSetIndex);
	}

	// Ignoring OTF_BackReference back reference
	public void setFilterSetSerialNumber(String serialNumber, int instrumentIndex, int filterSetIndex)
	{
		serialNumber = filter? DataTools.sanitize(serialNumber) : serialNumber;
		store.setFilterSetSerialNumber(serialNumber, instrumentIndex, filterSetIndex);
	}

	//
	// FilterSetRef property storage
	//
	// {u'OTF': {u'Instrument': {u'OME': None}}, u'Channel': {u'Pixels': {u'Image': {u'OME': None}}}}
	// Is multi path? True

	// 1:1
	// Is multi path? True
	// Ignoring ID property of reference FilterSetRef

	//
	// Group property storage
	//
	// {u'OME': None}
	// Is multi path? False

	public void setGroupContact(String contact, int groupIndex)
	{
		store.setGroupContact(contact, groupIndex);
	}

	// Ignoring Dataset_BackReference back reference
	public void setGroupDescription(String description, int groupIndex)
	{
		description = filter? DataTools.sanitize(description) : description;
		store.setGroupDescription(description, groupIndex);
	}

	// Ignoring Experimenter_BackReference back reference
	public void setGroupID(String id, int groupIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setGroupID(id, groupIndex);
	}

	// Ignoring Image_BackReference back reference
	public void setGroupLeader(String leader, int groupIndex)
	{
		store.setGroupLeader(leader, groupIndex);
	}

	public void setGroupName(String name, int groupIndex)
	{
		name = filter? DataTools.sanitize(name) : name;
		store.setGroupName(name, groupIndex);
	}

	// Ignoring Project_BackReference back reference
	//
	// GroupRef property storage
	//
	// {u'Project': {u'OME': None}, u'Image': {u'OME': None}, u'Experimenter': {u'OME': None}, u'Dataset': {u'OME': None}}
	// Is multi path? True

	// 1:1
	// Is multi path? True
	// Ignoring ID property of reference GroupRef

	//
	// Image property storage
	//
	// {u'OME': None}
	// Is multi path? False

	public void setImageAcquiredDate(String acquiredDate, int imageIndex)
	{
		acquiredDate = filter? DataTools.sanitize(acquiredDate) : acquiredDate;
		store.setImageAcquiredDate(acquiredDate, imageIndex);
	}

	public void setImageAnnotationRef(String annotation, int imageIndex, int annotationRefIndex)
	{
		store.setImageAnnotationRef(annotation, imageIndex, annotationRefIndex);
	}

	public void setImageDatasetRef(String dataset, int imageIndex, int datasetRefIndex)
	{
		store.setImageDatasetRef(dataset, imageIndex, datasetRefIndex);
	}

	public void setImageDescription(String description, int imageIndex)
	{
		description = filter? DataTools.sanitize(description) : description;
		store.setImageDescription(description, imageIndex);
	}

	public void setImageExperimentRef(String experiment, int imageIndex)
	{
		store.setImageExperimentRef(experiment, imageIndex);
	}

	public void setImageExperimenterRef(String experimenter, int imageIndex)
	{
		store.setImageExperimenterRef(experimenter, imageIndex);
	}

	public void setImageGroupRef(String group, int imageIndex)
	{
		store.setImageGroupRef(group, imageIndex);
	}

	public void setImageID(String id, int imageIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setImageID(id, imageIndex);
	}

	// Ignoring ImagingEnvironment element, complex property
	public void setImageInstrumentRef(String instrument, int imageIndex)
	{
		store.setImageInstrumentRef(instrument, imageIndex);
	}

	public void setImageMicrobeamManipulationRef(String microbeamManipulation, int imageIndex, int microbeamManipulationRefIndex)
	{
		store.setImageMicrobeamManipulationRef(microbeamManipulation, imageIndex, microbeamManipulationRefIndex);
	}

	public void setImageName(String name, int imageIndex)
	{
		name = filter? DataTools.sanitize(name) : name;
		store.setImageName(name, imageIndex);
	}

	// Ignoring ObjectiveSettings element, complex property
	// Ignoring Pixels element, complex property
	public void setImageROIRef(String roi, int imageIndex, int ROIRefIndex)
	{
		store.setImageROIRef(roi, imageIndex, ROIRefIndex);
	}

	// Ignoring StageLabel element, complex property
	// Ignoring WellSample_BackReference back reference
	//
	// ImageRef property storage
	//
	// {u'WellSample': {u'Well': {u'Plate': {u'OME': None}}}}
	// Is multi path? False

	// 1:1
	// Is multi path? False
	// Ignoring ID property of reference ImageRef

	//
	// ImagingEnvironment property storage
	//
	// {u'Image': {u'OME': None}}
	// Is multi path? False

	public void setImagingEnvironmentAirPressure(Double airPressure, int imageIndex)
	{
		store.setImagingEnvironmentAirPressure(airPressure, imageIndex);
	}

	public void setImagingEnvironmentCO2Percent(PercentFraction co2percent, int imageIndex)
	{
		store.setImagingEnvironmentCO2Percent(co2percent, imageIndex);
	}

	public void setImagingEnvironmentHumidity(PercentFraction humidity, int imageIndex)
	{
		store.setImagingEnvironmentHumidity(humidity, imageIndex);
	}

	public void setImagingEnvironmentTemperature(Double temperature, int imageIndex)
	{
		store.setImagingEnvironmentTemperature(temperature, imageIndex);
	}

	//
	// Instrument property storage
	//
	// {u'OME': None}
	// Is multi path? False

	// Ignoring Detector element, complex property
	// Ignoring Dichroic element, complex property
	// Ignoring Filter element, complex property
	// Ignoring FilterSet element, complex property
	public void setInstrumentID(String id, int instrumentIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setInstrumentID(id, instrumentIndex);
	}

	// Ignoring Image_BackReference back reference
	// Ignoring LightSource element, complex property
	// Ignoring Microscope element, complex property
	// Ignoring OTF element, complex property
	// Ignoring Objective element, complex property
	//
	// InstrumentRef property storage
	//
	// {u'Image': {u'OME': None}}
	// Is multi path? False

	// 1:1
	// Is multi path? False
	// Ignoring ID property of reference InstrumentRef

	//
	// Laser property storage
	//
	// {u'LightSource': {u'Instrument': {u'OME': None}}}
	// Is multi path? False

	// Ignoring Arc of parent abstract type
	// Ignoring Filament of parent abstract type
	// ID accessor from parent LightSource
	public void setLaserID(String id, int instrumentIndex, int lightSourceIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setLaserID(id, instrumentIndex, lightSourceIndex);
	}

	// Ignoring Laser of parent abstract type
	// Ignoring LightEmittingDiode of parent abstract type
	// LotNumber accessor from parent LightSource
	public void setLaserLotNumber(String lotNumber, int instrumentIndex, int lightSourceIndex)
	{
		lotNumber = filter? DataTools.sanitize(lotNumber) : lotNumber;
		store.setLaserLotNumber(lotNumber, instrumentIndex, lightSourceIndex);
	}

	// Manufacturer accessor from parent LightSource
	public void setLaserManufacturer(String manufacturer, int instrumentIndex, int lightSourceIndex)
	{
		manufacturer = filter? DataTools.sanitize(manufacturer) : manufacturer;
		store.setLaserManufacturer(manufacturer, instrumentIndex, lightSourceIndex);
	}

	// Model accessor from parent LightSource
	public void setLaserModel(String model, int instrumentIndex, int lightSourceIndex)
	{
		model = filter? DataTools.sanitize(model) : model;
		store.setLaserModel(model, instrumentIndex, lightSourceIndex);
	}

	// Power accessor from parent LightSource
	public void setLaserPower(Double power, int instrumentIndex, int lightSourceIndex)
	{
		store.setLaserPower(power, instrumentIndex, lightSourceIndex);
	}

	// SerialNumber accessor from parent LightSource
	public void setLaserSerialNumber(String serialNumber, int instrumentIndex, int lightSourceIndex)
	{
		serialNumber = filter? DataTools.sanitize(serialNumber) : serialNumber;
		store.setLaserSerialNumber(serialNumber, instrumentIndex, lightSourceIndex);
	}

	public void setLaserFrequencyMultiplication(PositiveInteger frequencyMultiplication, int instrumentIndex, int lightSourceIndex)
	{
		store.setLaserFrequencyMultiplication(frequencyMultiplication, instrumentIndex, lightSourceIndex);
	}

	public void setLaserLaserMedium(LaserMedium laserMedium, int instrumentIndex, int lightSourceIndex)
	{
		store.setLaserLaserMedium(laserMedium, instrumentIndex, lightSourceIndex);
	}

	public void setLaserPockelCell(Boolean pockelCell, int instrumentIndex, int lightSourceIndex)
	{
		store.setLaserPockelCell(pockelCell, instrumentIndex, lightSourceIndex);
	}

	public void setLaserPulse(Pulse pulse, int instrumentIndex, int lightSourceIndex)
	{
		store.setLaserPulse(pulse, instrumentIndex, lightSourceIndex);
	}

	public void setLaserPump(String pump, int instrumentIndex, int lightSourceIndex)
	{
		store.setLaserPump(pump, instrumentIndex, lightSourceIndex);
	}

	public void setLaserRepetitionRate(Double repetitionRate, int instrumentIndex, int lightSourceIndex)
	{
		store.setLaserRepetitionRate(repetitionRate, instrumentIndex, lightSourceIndex);
	}

	public void setLaserTuneable(Boolean tuneable, int instrumentIndex, int lightSourceIndex)
	{
		store.setLaserTuneable(tuneable, instrumentIndex, lightSourceIndex);
	}

	public void setLaserType(LaserType type, int instrumentIndex, int lightSourceIndex)
	{
		store.setLaserType(type, instrumentIndex, lightSourceIndex);
	}

	public void setLaserWavelength(PositiveInteger wavelength, int instrumentIndex, int lightSourceIndex)
	{
		store.setLaserWavelength(wavelength, instrumentIndex, lightSourceIndex);
	}

	//
	// Leader property storage
	//
	// {u'Group': {u'OME': None}}
	// Is multi path? False

	// 1:1
	// Is multi path? False
	// Ignoring ID property of reference Leader

	//
	// LightEmittingDiode property storage
	//
	// {u'LightSource': {u'Instrument': {u'OME': None}}}
	// Is multi path? False

	// Ignoring Arc of parent abstract type
	// Ignoring Filament of parent abstract type
	// ID accessor from parent LightSource
	public void setLightEmittingDiodeID(String id, int instrumentIndex, int lightSourceIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setLightEmittingDiodeID(id, instrumentIndex, lightSourceIndex);
	}

	// Ignoring Laser of parent abstract type
	// Ignoring LightEmittingDiode of parent abstract type
	// LotNumber accessor from parent LightSource
	public void setLightEmittingDiodeLotNumber(String lotNumber, int instrumentIndex, int lightSourceIndex)
	{
		lotNumber = filter? DataTools.sanitize(lotNumber) : lotNumber;
		store.setLightEmittingDiodeLotNumber(lotNumber, instrumentIndex, lightSourceIndex);
	}

	// Manufacturer accessor from parent LightSource
	public void setLightEmittingDiodeManufacturer(String manufacturer, int instrumentIndex, int lightSourceIndex)
	{
		manufacturer = filter? DataTools.sanitize(manufacturer) : manufacturer;
		store.setLightEmittingDiodeManufacturer(manufacturer, instrumentIndex, lightSourceIndex);
	}

	// Model accessor from parent LightSource
	public void setLightEmittingDiodeModel(String model, int instrumentIndex, int lightSourceIndex)
	{
		model = filter? DataTools.sanitize(model) : model;
		store.setLightEmittingDiodeModel(model, instrumentIndex, lightSourceIndex);
	}

	// Power accessor from parent LightSource
	public void setLightEmittingDiodePower(Double power, int instrumentIndex, int lightSourceIndex)
	{
		store.setLightEmittingDiodePower(power, instrumentIndex, lightSourceIndex);
	}

	// SerialNumber accessor from parent LightSource
	public void setLightEmittingDiodeSerialNumber(String serialNumber, int instrumentIndex, int lightSourceIndex)
	{
		serialNumber = filter? DataTools.sanitize(serialNumber) : serialNumber;
		store.setLightEmittingDiodeSerialNumber(serialNumber, instrumentIndex, lightSourceIndex);
	}

	//
	// LightPath property storage
	//
	// {u'Channel': {u'Pixels': {u'Image': {u'OME': None}}}}
	// Is multi path? False

	public void setLightPathDichroicRef(String dichroic, int imageIndex, int channelIndex)
	{
		store.setLightPathDichroicRef(dichroic, imageIndex, channelIndex);
	}

	public void setLightPathEmissionFilterRef(String emissionFilter, int imageIndex, int channelIndex, int emissionFilterRefIndex)
	{
		store.setLightPathEmissionFilterRef(emissionFilter, imageIndex, channelIndex, emissionFilterRefIndex);
	}

	public void setLightPathExcitationFilterRef(String excitationFilter, int imageIndex, int channelIndex, int excitationFilterRefIndex)
	{
		store.setLightPathExcitationFilterRef(excitationFilter, imageIndex, channelIndex, excitationFilterRefIndex);
	}

	//
	// LightSourceSettings property storage
	//
	// {u'Channel': {u'Pixels': {u'Image': {u'OME': None}}}, u'MicrobeamManipulation': {u'Experiment': {u'OME': None}}}
	// Is multi path? True

	public void setChannelLightSourceSettingsAttenuation(PercentFraction attenuation, int imageIndex, int channelIndex)
	{
		store.setChannelLightSourceSettingsAttenuation(attenuation, imageIndex, channelIndex);
	}

	public void setMicrobeamManipulationLightSourceSettingsAttenuation(PercentFraction attenuation, int experimentIndex, int microbeamManipulationIndex, int lightSourceSettingsIndex)
	{
		store.setMicrobeamManipulationLightSourceSettingsAttenuation(attenuation, experimentIndex, microbeamManipulationIndex, lightSourceSettingsIndex);
	}

	public void setChannelLightSourceSettingsID(String id, int imageIndex, int channelIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setChannelLightSourceSettingsID(id, imageIndex, channelIndex);
	}

	public void setMicrobeamManipulationLightSourceSettingsID(String id, int experimentIndex, int microbeamManipulationIndex, int lightSourceSettingsIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setMicrobeamManipulationLightSourceSettingsID(id, experimentIndex, microbeamManipulationIndex, lightSourceSettingsIndex);
	}

	public void setChannelLightSourceSettingsWavelength(PositiveInteger wavelength, int imageIndex, int channelIndex)
	{
		store.setChannelLightSourceSettingsWavelength(wavelength, imageIndex, channelIndex);
	}

	public void setMicrobeamManipulationLightSourceSettingsWavelength(PositiveInteger wavelength, int experimentIndex, int microbeamManipulationIndex, int lightSourceSettingsIndex)
	{
		store.setMicrobeamManipulationLightSourceSettingsWavelength(wavelength, experimentIndex, microbeamManipulationIndex, lightSourceSettingsIndex);
	}

	//
	// Line property storage
	//
	// {u'Shape': {u'Union': {u'ROI': {u'OME': None}}}}
	// Is multi path? False

	// Description accessor from parent Shape
	public void setLineDescription(String description, int ROIIndex, int shapeIndex)
	{
		description = filter? DataTools.sanitize(description) : description;
		store.setLineDescription(description, ROIIndex, shapeIndex);
	}

	// Ignoring Ellipse of parent abstract type
	// Fill accessor from parent Shape
	public void setLineFill(Integer fill, int ROIIndex, int shapeIndex)
	{
		store.setLineFill(fill, ROIIndex, shapeIndex);
	}

	// Ignoring FillRule of parent abstract type
	// Ignoring FontFamily of parent abstract type
	// FontSize accessor from parent Shape
	public void setLineFontSize(NonNegativeInteger fontSize, int ROIIndex, int shapeIndex)
	{
		store.setLineFontSize(fontSize, ROIIndex, shapeIndex);
	}

	// Ignoring FontStyle of parent abstract type
	// ID accessor from parent Shape
	public void setLineID(String id, int ROIIndex, int shapeIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setLineID(id, ROIIndex, shapeIndex);
	}

	// Label accessor from parent Shape
	public void setLineLabel(String label, int ROIIndex, int shapeIndex)
	{
		label = filter? DataTools.sanitize(label) : label;
		store.setLineLabel(label, ROIIndex, shapeIndex);
	}

	// Ignoring Line of parent abstract type
	// Ignoring LineCap of parent abstract type
	// Ignoring MarkerEnd of parent abstract type
	// Ignoring MarkerStart of parent abstract type
	// Ignoring Mask of parent abstract type
	// Name accessor from parent Shape
	public void setLineName(String name, int ROIIndex, int shapeIndex)
	{
		name = filter? DataTools.sanitize(name) : name;
		store.setLineName(name, ROIIndex, shapeIndex);
	}

	// Ignoring Path of parent abstract type
	// Ignoring Point of parent abstract type
	// Ignoring Polyline of parent abstract type
	// Ignoring Rectangle of parent abstract type
	// Stroke accessor from parent Shape
	public void setLineStroke(Integer stroke, int ROIIndex, int shapeIndex)
	{
		store.setLineStroke(stroke, ROIIndex, shapeIndex);
	}

	// StrokeDashArray accessor from parent Shape
	public void setLineStrokeDashArray(String strokeDashArray, int ROIIndex, int shapeIndex)
	{
		strokeDashArray = filter? DataTools.sanitize(strokeDashArray) : strokeDashArray;
		store.setLineStrokeDashArray(strokeDashArray, ROIIndex, shapeIndex);
	}

	// StrokeWidth accessor from parent Shape
	public void setLineStrokeWidth(Double strokeWidth, int ROIIndex, int shapeIndex)
	{
		store.setLineStrokeWidth(strokeWidth, ROIIndex, shapeIndex);
	}

	// Ignoring Text of parent abstract type
	// TheC accessor from parent Shape
	public void setLineTheC(NonNegativeInteger theC, int ROIIndex, int shapeIndex)
	{
		store.setLineTheC(theC, ROIIndex, shapeIndex);
	}

	// TheT accessor from parent Shape
	public void setLineTheT(NonNegativeInteger theT, int ROIIndex, int shapeIndex)
	{
		store.setLineTheT(theT, ROIIndex, shapeIndex);
	}

	// TheZ accessor from parent Shape
	public void setLineTheZ(NonNegativeInteger theZ, int ROIIndex, int shapeIndex)
	{
		store.setLineTheZ(theZ, ROIIndex, shapeIndex);
	}

	// Transform accessor from parent Shape
	public void setLineTransform(String transform, int ROIIndex, int shapeIndex)
	{
		transform = filter? DataTools.sanitize(transform) : transform;
		store.setLineTransform(transform, ROIIndex, shapeIndex);
	}

	public void setLineX1(Double x1, int ROIIndex, int shapeIndex)
	{
		store.setLineX1(x1, ROIIndex, shapeIndex);
	}

	public void setLineX2(Double x2, int ROIIndex, int shapeIndex)
	{
		store.setLineX2(x2, ROIIndex, shapeIndex);
	}

	public void setLineY1(Double y1, int ROIIndex, int shapeIndex)
	{
		store.setLineY1(y1, ROIIndex, shapeIndex);
	}

	public void setLineY2(Double y2, int ROIIndex, int shapeIndex)
	{
		store.setLineY2(y2, ROIIndex, shapeIndex);
	}

	//
	// ListAnnotation property storage
	//
	// {u'StructuredAnnotations': {u'OME': None}}
	// Is multi path? False

	public void setListAnnotationAnnotationRef(String annotation, int listAnnotationIndex, int annotationRefIndex)
	{
		store.setListAnnotationAnnotationRef(annotation, listAnnotationIndex, annotationRefIndex);
	}

	// Ignoring Channel_BackReference back reference
	// Ignoring Dataset_BackReference back reference
	public void setListAnnotationDescription(String description, int listAnnotationIndex)
	{
		description = filter? DataTools.sanitize(description) : description;
		store.setListAnnotationDescription(description, listAnnotationIndex);
	}

	// Ignoring Experimenter_BackReference back reference
	public void setListAnnotationID(String id, int listAnnotationIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setListAnnotationID(id, listAnnotationIndex);
	}

	// Ignoring Image_BackReference back reference
	public void setListAnnotationNamespace(String namespace, int listAnnotationIndex)
	{
		namespace = filter? DataTools.sanitize(namespace) : namespace;
		store.setListAnnotationNamespace(namespace, listAnnotationIndex);
	}

	// Ignoring Pixels_BackReference back reference
	// Ignoring Plane_BackReference back reference
	// Ignoring PlateAcquisition_BackReference back reference
	// Ignoring Plate_BackReference back reference
	// Ignoring Project_BackReference back reference
	// Ignoring ROI_BackReference back reference
	// Ignoring Reagent_BackReference back reference
	// Ignoring Screen_BackReference back reference
	// Ignoring WellSample_BackReference back reference
	// Ignoring Well_BackReference back reference
	//
	// LongAnnotation property storage
	//
	// {u'StructuredAnnotations': {u'OME': None}}
	// Is multi path? False

	public void setLongAnnotationAnnotationRef(String annotation, int longAnnotationIndex, int annotationRefIndex)
	{
		store.setLongAnnotationAnnotationRef(annotation, longAnnotationIndex, annotationRefIndex);
	}

	// Ignoring Channel_BackReference back reference
	// Ignoring Dataset_BackReference back reference
	public void setLongAnnotationDescription(String description, int longAnnotationIndex)
	{
		description = filter? DataTools.sanitize(description) : description;
		store.setLongAnnotationDescription(description, longAnnotationIndex);
	}

	// Ignoring Experimenter_BackReference back reference
	public void setLongAnnotationID(String id, int longAnnotationIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setLongAnnotationID(id, longAnnotationIndex);
	}

	// Ignoring Image_BackReference back reference
	public void setLongAnnotationNamespace(String namespace, int longAnnotationIndex)
	{
		namespace = filter? DataTools.sanitize(namespace) : namespace;
		store.setLongAnnotationNamespace(namespace, longAnnotationIndex);
	}

	// Ignoring Pixels_BackReference back reference
	// Ignoring Plane_BackReference back reference
	// Ignoring PlateAcquisition_BackReference back reference
	// Ignoring Plate_BackReference back reference
	// Ignoring Project_BackReference back reference
	// Ignoring ROI_BackReference back reference
	// Ignoring Reagent_BackReference back reference
	// Ignoring Screen_BackReference back reference
	public void setLongAnnotationValue(Long value, int longAnnotationIndex)
	{
		store.setLongAnnotationValue(value, longAnnotationIndex);
	}

	// Ignoring WellSample_BackReference back reference
	// Ignoring Well_BackReference back reference
	//
	// Mask property storage
	//
	// {u'Shape': {u'Union': {u'ROI': {u'OME': None}}}}
	// Is multi path? False

	// Description accessor from parent Shape
	public void setMaskDescription(String description, int ROIIndex, int shapeIndex)
	{
		description = filter? DataTools.sanitize(description) : description;
		store.setMaskDescription(description, ROIIndex, shapeIndex);
	}

	// Ignoring Ellipse of parent abstract type
	// Fill accessor from parent Shape
	public void setMaskFill(Integer fill, int ROIIndex, int shapeIndex)
	{
		store.setMaskFill(fill, ROIIndex, shapeIndex);
	}

	// Ignoring FillRule of parent abstract type
	// Ignoring FontFamily of parent abstract type
	// FontSize accessor from parent Shape
	public void setMaskFontSize(NonNegativeInteger fontSize, int ROIIndex, int shapeIndex)
	{
		store.setMaskFontSize(fontSize, ROIIndex, shapeIndex);
	}

	// Ignoring FontStyle of parent abstract type
	// ID accessor from parent Shape
	public void setMaskID(String id, int ROIIndex, int shapeIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setMaskID(id, ROIIndex, shapeIndex);
	}

	// Label accessor from parent Shape
	public void setMaskLabel(String label, int ROIIndex, int shapeIndex)
	{
		label = filter? DataTools.sanitize(label) : label;
		store.setMaskLabel(label, ROIIndex, shapeIndex);
	}

	// Ignoring Line of parent abstract type
	// Ignoring LineCap of parent abstract type
	// Ignoring MarkerEnd of parent abstract type
	// Ignoring MarkerStart of parent abstract type
	// Ignoring Mask of parent abstract type
	// Name accessor from parent Shape
	public void setMaskName(String name, int ROIIndex, int shapeIndex)
	{
		name = filter? DataTools.sanitize(name) : name;
		store.setMaskName(name, ROIIndex, shapeIndex);
	}

	// Ignoring Path of parent abstract type
	// Ignoring Point of parent abstract type
	// Ignoring Polyline of parent abstract type
	// Ignoring Rectangle of parent abstract type
	// Stroke accessor from parent Shape
	public void setMaskStroke(Integer stroke, int ROIIndex, int shapeIndex)
	{
		store.setMaskStroke(stroke, ROIIndex, shapeIndex);
	}

	// StrokeDashArray accessor from parent Shape
	public void setMaskStrokeDashArray(String strokeDashArray, int ROIIndex, int shapeIndex)
	{
		strokeDashArray = filter? DataTools.sanitize(strokeDashArray) : strokeDashArray;
		store.setMaskStrokeDashArray(strokeDashArray, ROIIndex, shapeIndex);
	}

	// StrokeWidth accessor from parent Shape
	public void setMaskStrokeWidth(Double strokeWidth, int ROIIndex, int shapeIndex)
	{
		store.setMaskStrokeWidth(strokeWidth, ROIIndex, shapeIndex);
	}

	// Ignoring Text of parent abstract type
	// TheC accessor from parent Shape
	public void setMaskTheC(NonNegativeInteger theC, int ROIIndex, int shapeIndex)
	{
		store.setMaskTheC(theC, ROIIndex, shapeIndex);
	}

	// TheT accessor from parent Shape
	public void setMaskTheT(NonNegativeInteger theT, int ROIIndex, int shapeIndex)
	{
		store.setMaskTheT(theT, ROIIndex, shapeIndex);
	}

	// TheZ accessor from parent Shape
	public void setMaskTheZ(NonNegativeInteger theZ, int ROIIndex, int shapeIndex)
	{
		store.setMaskTheZ(theZ, ROIIndex, shapeIndex);
	}

	// Transform accessor from parent Shape
	public void setMaskTransform(String transform, int ROIIndex, int shapeIndex)
	{
		transform = filter? DataTools.sanitize(transform) : transform;
		store.setMaskTransform(transform, ROIIndex, shapeIndex);
	}

	// Ignoring BinData element, complex property
	public void setMaskHeight(Double height, int ROIIndex, int shapeIndex)
	{
		store.setMaskHeight(height, ROIIndex, shapeIndex);
	}

	public void setMaskWidth(Double width, int ROIIndex, int shapeIndex)
	{
		store.setMaskWidth(width, ROIIndex, shapeIndex);
	}

	public void setMaskX(Double x, int ROIIndex, int shapeIndex)
	{
		store.setMaskX(x, ROIIndex, shapeIndex);
	}

	public void setMaskY(Double y, int ROIIndex, int shapeIndex)
	{
		store.setMaskY(y, ROIIndex, shapeIndex);
	}

	//
	// MetadataOnly property storage
	//
	// {u'Pixels': {u'Image': {u'OME': None}}}
	// Is multi path? False

	//
	// MicrobeamManipulation property storage
	//
	// {u'Experiment': {u'OME': None}}
	// Is multi path? False

	public void setMicrobeamManipulationExperimenterRef(String experimenter, int experimentIndex, int microbeamManipulationIndex)
	{
		store.setMicrobeamManipulationExperimenterRef(experimenter, experimentIndex, microbeamManipulationIndex);
	}

	public void setMicrobeamManipulationID(String id, int experimentIndex, int microbeamManipulationIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setMicrobeamManipulationID(id, experimentIndex, microbeamManipulationIndex);
	}

	// Ignoring Image_BackReference back reference
	// Ignoring LightSourceSettings element, complex property
	public void setMicrobeamManipulationROIRef(String roi, int experimentIndex, int microbeamManipulationIndex, int ROIRefIndex)
	{
		store.setMicrobeamManipulationROIRef(roi, experimentIndex, microbeamManipulationIndex, ROIRefIndex);
	}

	public void setMicrobeamManipulationType(MicrobeamManipulationType type, int experimentIndex, int microbeamManipulationIndex)
	{
		store.setMicrobeamManipulationType(type, experimentIndex, microbeamManipulationIndex);
	}

	//
	// MicrobeamManipulationRef property storage
	//
	// {u'Image': {u'OME': None}}
	// Is multi path? False

	// 1:1
	// Is multi path? False
	// Ignoring ID property of reference MicrobeamManipulationRef

	//
	// Microscope property storage
	//
	// {u'Instrument': {u'OME': None}}
	// Is multi path? False

	public void setMicroscopeLotNumber(String lotNumber, int instrumentIndex)
	{
		lotNumber = filter? DataTools.sanitize(lotNumber) : lotNumber;
		store.setMicroscopeLotNumber(lotNumber, instrumentIndex);
	}

	public void setMicroscopeManufacturer(String manufacturer, int instrumentIndex)
	{
		manufacturer = filter? DataTools.sanitize(manufacturer) : manufacturer;
		store.setMicroscopeManufacturer(manufacturer, instrumentIndex);
	}

	public void setMicroscopeModel(String model, int instrumentIndex)
	{
		model = filter? DataTools.sanitize(model) : model;
		store.setMicroscopeModel(model, instrumentIndex);
	}

	public void setMicroscopeSerialNumber(String serialNumber, int instrumentIndex)
	{
		serialNumber = filter? DataTools.sanitize(serialNumber) : serialNumber;
		store.setMicroscopeSerialNumber(serialNumber, instrumentIndex);
	}

	public void setMicroscopeType(MicroscopeType type, int instrumentIndex)
	{
		store.setMicroscopeType(type, instrumentIndex);
	}

	//
	// OTF property storage
	//
	// {u'Instrument': {u'OME': None}}
	// Is multi path? False

	// Ignoring BinaryFile element, complex property
	// Ignoring Channel_BackReference back reference
	public void setOTFFilterSetRef(String filterSet, int instrumentIndex, int OTFIndex)
	{
		store.setOTFFilterSetRef(filterSet, instrumentIndex, OTFIndex);
	}

	public void setOTFID(String id, int instrumentIndex, int OTFIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setOTFID(id, instrumentIndex, OTFIndex);
	}

	// Ignoring ObjectiveSettings element, complex property
	public void setOTFOpticalAxisAveraged(Boolean opticalAxisAveraged, int instrumentIndex, int OTFIndex)
	{
		store.setOTFOpticalAxisAveraged(opticalAxisAveraged, instrumentIndex, OTFIndex);
	}

	public void setOTFSizeX(PositiveInteger sizeX, int instrumentIndex, int OTFIndex)
	{
		store.setOTFSizeX(sizeX, instrumentIndex, OTFIndex);
	}

	public void setOTFSizeY(PositiveInteger sizeY, int instrumentIndex, int OTFIndex)
	{
		store.setOTFSizeY(sizeY, instrumentIndex, OTFIndex);
	}

	public void setOTFType(PixelType type, int instrumentIndex, int OTFIndex)
	{
		store.setOTFType(type, instrumentIndex, OTFIndex);
	}

	//
	// OTFRef property storage
	//
	// {u'Channel': {u'Pixels': {u'Image': {u'OME': None}}}}
	// Is multi path? False

	// 1:1
	// Is multi path? False
	// Ignoring ID property of reference OTFRef

	//
	// Objective property storage
	//
	// {u'Instrument': {u'OME': None}}
	// Is multi path? False

	public void setObjectiveCalibratedMagnification(Double calibratedMagnification, int instrumentIndex, int objectiveIndex)
	{
		store.setObjectiveCalibratedMagnification(calibratedMagnification, instrumentIndex, objectiveIndex);
	}

	public void setObjectiveCorrection(Correction correction, int instrumentIndex, int objectiveIndex)
	{
		store.setObjectiveCorrection(correction, instrumentIndex, objectiveIndex);
	}

	public void setObjectiveID(String id, int instrumentIndex, int objectiveIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setObjectiveID(id, instrumentIndex, objectiveIndex);
	}

	public void setObjectiveImmersion(Immersion immersion, int instrumentIndex, int objectiveIndex)
	{
		store.setObjectiveImmersion(immersion, instrumentIndex, objectiveIndex);
	}

	public void setObjectiveIris(Boolean iris, int instrumentIndex, int objectiveIndex)
	{
		store.setObjectiveIris(iris, instrumentIndex, objectiveIndex);
	}

	public void setObjectiveLensNA(Double lensNA, int instrumentIndex, int objectiveIndex)
	{
		store.setObjectiveLensNA(lensNA, instrumentIndex, objectiveIndex);
	}

	public void setObjectiveLotNumber(String lotNumber, int instrumentIndex, int objectiveIndex)
	{
		lotNumber = filter? DataTools.sanitize(lotNumber) : lotNumber;
		store.setObjectiveLotNumber(lotNumber, instrumentIndex, objectiveIndex);
	}

	public void setObjectiveManufacturer(String manufacturer, int instrumentIndex, int objectiveIndex)
	{
		manufacturer = filter? DataTools.sanitize(manufacturer) : manufacturer;
		store.setObjectiveManufacturer(manufacturer, instrumentIndex, objectiveIndex);
	}

	public void setObjectiveModel(String model, int instrumentIndex, int objectiveIndex)
	{
		model = filter? DataTools.sanitize(model) : model;
		store.setObjectiveModel(model, instrumentIndex, objectiveIndex);
	}

	public void setObjectiveNominalMagnification(PositiveInteger nominalMagnification, int instrumentIndex, int objectiveIndex)
	{
		store.setObjectiveNominalMagnification(nominalMagnification, instrumentIndex, objectiveIndex);
	}

	public void setObjectiveSerialNumber(String serialNumber, int instrumentIndex, int objectiveIndex)
	{
		serialNumber = filter? DataTools.sanitize(serialNumber) : serialNumber;
		store.setObjectiveSerialNumber(serialNumber, instrumentIndex, objectiveIndex);
	}

	public void setObjectiveWorkingDistance(Double workingDistance, int instrumentIndex, int objectiveIndex)
	{
		store.setObjectiveWorkingDistance(workingDistance, instrumentIndex, objectiveIndex);
	}

	//
	// ObjectiveSettings property storage
	//
	// {u'Image': {u'OME': None}, u'OTF': {u'Instrument': {u'OME': None}}}
	// Is multi path? True

	public void setImageObjectiveSettingsCorrectionCollar(Double correctionCollar, int imageIndex)
	{
		store.setImageObjectiveSettingsCorrectionCollar(correctionCollar, imageIndex);
	}

	public void setOTFObjectiveSettingsCorrectionCollar(Double correctionCollar, int instrumentIndex, int OTFIndex)
	{
		store.setOTFObjectiveSettingsCorrectionCollar(correctionCollar, instrumentIndex, OTFIndex);
	}

	public void setImageObjectiveSettingsID(String id, int imageIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setImageObjectiveSettingsID(id, imageIndex);
	}

	public void setOTFObjectiveSettingsID(String id, int instrumentIndex, int OTFIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setOTFObjectiveSettingsID(id, instrumentIndex, OTFIndex);
	}

	public void setImageObjectiveSettingsMedium(Medium medium, int imageIndex)
	{
		store.setImageObjectiveSettingsMedium(medium, imageIndex);
	}

	public void setOTFObjectiveSettingsMedium(Medium medium, int instrumentIndex, int OTFIndex)
	{
		store.setOTFObjectiveSettingsMedium(medium, instrumentIndex, OTFIndex);
	}

	public void setImageObjectiveSettingsRefractiveIndex(Double refractiveIndex, int imageIndex)
	{
		store.setImageObjectiveSettingsRefractiveIndex(refractiveIndex, imageIndex);
	}

	public void setOTFObjectiveSettingsRefractiveIndex(Double refractiveIndex, int instrumentIndex, int OTFIndex)
	{
		store.setOTFObjectiveSettingsRefractiveIndex(refractiveIndex, instrumentIndex, OTFIndex);
	}

	//
	// Path property storage
	//
	// {u'Shape': {u'Union': {u'ROI': {u'OME': None}}}}
	// Is multi path? False

	// Description accessor from parent Shape
	public void setPathDescription(String description, int ROIIndex, int shapeIndex)
	{
		description = filter? DataTools.sanitize(description) : description;
		store.setPathDescription(description, ROIIndex, shapeIndex);
	}

	// Ignoring Ellipse of parent abstract type
	// Fill accessor from parent Shape
	public void setPathFill(Integer fill, int ROIIndex, int shapeIndex)
	{
		store.setPathFill(fill, ROIIndex, shapeIndex);
	}

	// Ignoring FillRule of parent abstract type
	// Ignoring FontFamily of parent abstract type
	// FontSize accessor from parent Shape
	public void setPathFontSize(NonNegativeInteger fontSize, int ROIIndex, int shapeIndex)
	{
		store.setPathFontSize(fontSize, ROIIndex, shapeIndex);
	}

	// Ignoring FontStyle of parent abstract type
	// ID accessor from parent Shape
	public void setPathID(String id, int ROIIndex, int shapeIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setPathID(id, ROIIndex, shapeIndex);
	}

	// Label accessor from parent Shape
	public void setPathLabel(String label, int ROIIndex, int shapeIndex)
	{
		label = filter? DataTools.sanitize(label) : label;
		store.setPathLabel(label, ROIIndex, shapeIndex);
	}

	// Ignoring Line of parent abstract type
	// Ignoring LineCap of parent abstract type
	// Ignoring MarkerEnd of parent abstract type
	// Ignoring MarkerStart of parent abstract type
	// Ignoring Mask of parent abstract type
	// Name accessor from parent Shape
	public void setPathName(String name, int ROIIndex, int shapeIndex)
	{
		name = filter? DataTools.sanitize(name) : name;
		store.setPathName(name, ROIIndex, shapeIndex);
	}

	// Ignoring Path of parent abstract type
	// Ignoring Point of parent abstract type
	// Ignoring Polyline of parent abstract type
	// Ignoring Rectangle of parent abstract type
	// Stroke accessor from parent Shape
	public void setPathStroke(Integer stroke, int ROIIndex, int shapeIndex)
	{
		store.setPathStroke(stroke, ROIIndex, shapeIndex);
	}

	// StrokeDashArray accessor from parent Shape
	public void setPathStrokeDashArray(String strokeDashArray, int ROIIndex, int shapeIndex)
	{
		strokeDashArray = filter? DataTools.sanitize(strokeDashArray) : strokeDashArray;
		store.setPathStrokeDashArray(strokeDashArray, ROIIndex, shapeIndex);
	}

	// StrokeWidth accessor from parent Shape
	public void setPathStrokeWidth(Double strokeWidth, int ROIIndex, int shapeIndex)
	{
		store.setPathStrokeWidth(strokeWidth, ROIIndex, shapeIndex);
	}

	// Ignoring Text of parent abstract type
	// TheC accessor from parent Shape
	public void setPathTheC(NonNegativeInteger theC, int ROIIndex, int shapeIndex)
	{
		store.setPathTheC(theC, ROIIndex, shapeIndex);
	}

	// TheT accessor from parent Shape
	public void setPathTheT(NonNegativeInteger theT, int ROIIndex, int shapeIndex)
	{
		store.setPathTheT(theT, ROIIndex, shapeIndex);
	}

	// TheZ accessor from parent Shape
	public void setPathTheZ(NonNegativeInteger theZ, int ROIIndex, int shapeIndex)
	{
		store.setPathTheZ(theZ, ROIIndex, shapeIndex);
	}

	// Transform accessor from parent Shape
	public void setPathTransform(String transform, int ROIIndex, int shapeIndex)
	{
		transform = filter? DataTools.sanitize(transform) : transform;
		store.setPathTransform(transform, ROIIndex, shapeIndex);
	}

	public void setPathDefinition(String definition, int ROIIndex, int shapeIndex)
	{
		definition = filter? DataTools.sanitize(definition) : definition;
		store.setPathDefinition(definition, ROIIndex, shapeIndex);
	}

	//
	// Pixels property storage
	//
	// {u'Image': {u'OME': None}}
	// Is multi path? False

	public void setPixelsAnnotationRef(String annotation, int imageIndex, int annotationRefIndex)
	{
		store.setPixelsAnnotationRef(annotation, imageIndex, annotationRefIndex);
	}

	// Ignoring BinData element, complex property
	// Ignoring Channel element, complex property
	public void setPixelsDimensionOrder(DimensionOrder dimensionOrder, int imageIndex)
	{
		store.setPixelsDimensionOrder(dimensionOrder, imageIndex);
	}

	public void setPixelsID(String id, int imageIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setPixelsID(id, imageIndex);
	}

	// Ignoring MetadataOnly element, complex property
	public void setPixelsPhysicalSizeX(Double physicalSizeX, int imageIndex)
	{
		store.setPixelsPhysicalSizeX(physicalSizeX, imageIndex);
	}

	public void setPixelsPhysicalSizeY(Double physicalSizeY, int imageIndex)
	{
		store.setPixelsPhysicalSizeY(physicalSizeY, imageIndex);
	}

	public void setPixelsPhysicalSizeZ(Double physicalSizeZ, int imageIndex)
	{
		store.setPixelsPhysicalSizeZ(physicalSizeZ, imageIndex);
	}

	// Ignoring Plane element, complex property
	public void setPixelsSizeC(PositiveInteger sizeC, int imageIndex)
	{
		store.setPixelsSizeC(sizeC, imageIndex);
	}

	public void setPixelsSizeT(PositiveInteger sizeT, int imageIndex)
	{
		store.setPixelsSizeT(sizeT, imageIndex);
	}

	public void setPixelsSizeX(PositiveInteger sizeX, int imageIndex)
	{
		store.setPixelsSizeX(sizeX, imageIndex);
	}

	public void setPixelsSizeY(PositiveInteger sizeY, int imageIndex)
	{
		store.setPixelsSizeY(sizeY, imageIndex);
	}

	public void setPixelsSizeZ(PositiveInteger sizeZ, int imageIndex)
	{
		store.setPixelsSizeZ(sizeZ, imageIndex);
	}

	// Ignoring TiffData element, complex property
	public void setPixelsTimeIncrement(Double timeIncrement, int imageIndex)
	{
		store.setPixelsTimeIncrement(timeIncrement, imageIndex);
	}

	public void setPixelsType(PixelType type, int imageIndex)
	{
		store.setPixelsType(type, imageIndex);
	}

	//
	// Plane property storage
	//
	// {u'Pixels': {u'Image': {u'OME': None}}}
	// Is multi path? False

	public void setPlaneAnnotationRef(String annotation, int imageIndex, int planeIndex, int annotationRefIndex)
	{
		store.setPlaneAnnotationRef(annotation, imageIndex, planeIndex, annotationRefIndex);
	}

	public void setPlaneDeltaT(Double deltaT, int imageIndex, int planeIndex)
	{
		store.setPlaneDeltaT(deltaT, imageIndex, planeIndex);
	}

	public void setPlaneExposureTime(Double exposureTime, int imageIndex, int planeIndex)
	{
		store.setPlaneExposureTime(exposureTime, imageIndex, planeIndex);
	}

	public void setPlaneHashSHA1(String hashSHA1, int imageIndex, int planeIndex)
	{
		hashSHA1 = filter? DataTools.sanitize(hashSHA1) : hashSHA1;
		store.setPlaneHashSHA1(hashSHA1, imageIndex, planeIndex);
	}

	public void setPlanePositionX(Double positionX, int imageIndex, int planeIndex)
	{
		store.setPlanePositionX(positionX, imageIndex, planeIndex);
	}

	public void setPlanePositionY(Double positionY, int imageIndex, int planeIndex)
	{
		store.setPlanePositionY(positionY, imageIndex, planeIndex);
	}

	public void setPlanePositionZ(Double positionZ, int imageIndex, int planeIndex)
	{
		store.setPlanePositionZ(positionZ, imageIndex, planeIndex);
	}

	public void setPlaneTheC(NonNegativeInteger theC, int imageIndex, int planeIndex)
	{
		store.setPlaneTheC(theC, imageIndex, planeIndex);
	}

	public void setPlaneTheT(NonNegativeInteger theT, int imageIndex, int planeIndex)
	{
		store.setPlaneTheT(theT, imageIndex, planeIndex);
	}

	public void setPlaneTheZ(NonNegativeInteger theZ, int imageIndex, int planeIndex)
	{
		store.setPlaneTheZ(theZ, imageIndex, planeIndex);
	}

	//
	// Plate property storage
	//
	// {u'OME': None}
	// Is multi path? False

	public void setPlateAnnotationRef(String annotation, int plateIndex, int annotationRefIndex)
	{
		store.setPlateAnnotationRef(annotation, plateIndex, annotationRefIndex);
	}

	public void setPlateColumnNamingConvention(NamingConvention columnNamingConvention, int plateIndex)
	{
		store.setPlateColumnNamingConvention(columnNamingConvention, plateIndex);
	}

	public void setPlateColumns(PositiveInteger columns, int plateIndex)
	{
		store.setPlateColumns(columns, plateIndex);
	}

	public void setPlateDescription(String description, int plateIndex)
	{
		description = filter? DataTools.sanitize(description) : description;
		store.setPlateDescription(description, plateIndex);
	}

	public void setPlateExternalIdentifier(String externalIdentifier, int plateIndex)
	{
		externalIdentifier = filter? DataTools.sanitize(externalIdentifier) : externalIdentifier;
		store.setPlateExternalIdentifier(externalIdentifier, plateIndex);
	}

	public void setPlateID(String id, int plateIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setPlateID(id, plateIndex);
	}

	public void setPlateName(String name, int plateIndex)
	{
		name = filter? DataTools.sanitize(name) : name;
		store.setPlateName(name, plateIndex);
	}

	// Ignoring PlateAcquisition element, complex property
	public void setPlateRowNamingConvention(NamingConvention rowNamingConvention, int plateIndex)
	{
		store.setPlateRowNamingConvention(rowNamingConvention, plateIndex);
	}

	public void setPlateRows(PositiveInteger rows, int plateIndex)
	{
		store.setPlateRows(rows, plateIndex);
	}

	public void setPlateScreenRef(String screen, int plateIndex, int screenRefIndex)
	{
		store.setPlateScreenRef(screen, plateIndex, screenRefIndex);
	}

	public void setPlateStatus(String status, int plateIndex)
	{
		status = filter? DataTools.sanitize(status) : status;
		store.setPlateStatus(status, plateIndex);
	}

	// Ignoring Well element, complex property
	public void setPlateWellOriginX(Double wellOriginX, int plateIndex)
	{
		store.setPlateWellOriginX(wellOriginX, plateIndex);
	}

	public void setPlateWellOriginY(Double wellOriginY, int plateIndex)
	{
		store.setPlateWellOriginY(wellOriginY, plateIndex);
	}

	//
	// PlateAcquisition property storage
	//
	// {u'Plate': {u'OME': None}}
	// Is multi path? False

	public void setPlateAcquisitionAnnotationRef(String annotation, int plateIndex, int plateAcquisitionIndex, int annotationRefIndex)
	{
		store.setPlateAcquisitionAnnotationRef(annotation, plateIndex, plateAcquisitionIndex, annotationRefIndex);
	}

	public void setPlateAcquisitionDescription(String description, int plateIndex, int plateAcquisitionIndex)
	{
		description = filter? DataTools.sanitize(description) : description;
		store.setPlateAcquisitionDescription(description, plateIndex, plateAcquisitionIndex);
	}

	public void setPlateAcquisitionEndTime(String endTime, int plateIndex, int plateAcquisitionIndex)
	{
		endTime = filter? DataTools.sanitize(endTime) : endTime;
		store.setPlateAcquisitionEndTime(endTime, plateIndex, plateAcquisitionIndex);
	}

	public void setPlateAcquisitionID(String id, int plateIndex, int plateAcquisitionIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setPlateAcquisitionID(id, plateIndex, plateAcquisitionIndex);
	}

	public void setPlateAcquisitionMaximumFieldCount(PositiveInteger maximumFieldCount, int plateIndex, int plateAcquisitionIndex)
	{
		store.setPlateAcquisitionMaximumFieldCount(maximumFieldCount, plateIndex, plateAcquisitionIndex);
	}

	public void setPlateAcquisitionName(String name, int plateIndex, int plateAcquisitionIndex)
	{
		name = filter? DataTools.sanitize(name) : name;
		store.setPlateAcquisitionName(name, plateIndex, plateAcquisitionIndex);
	}

	public void setPlateAcquisitionStartTime(String startTime, int plateIndex, int plateAcquisitionIndex)
	{
		startTime = filter? DataTools.sanitize(startTime) : startTime;
		store.setPlateAcquisitionStartTime(startTime, plateIndex, plateAcquisitionIndex);
	}

	public void setPlateAcquisitionWellSampleRef(String wellSample, int plateIndex, int plateAcquisitionIndex, int wellSampleRefIndex)
	{
		store.setPlateAcquisitionWellSampleRef(wellSample, plateIndex, plateAcquisitionIndex, wellSampleRefIndex);
	}

	//
	// PlateRef property storage
	//
	// {u'Screen': {u'OME': None}}
	// Is multi path? False

	// 1:1
	// Is multi path? False
	// Ignoring ID property of reference PlateRef

	//
	// Point property storage
	//
	// {u'Shape': {u'Union': {u'ROI': {u'OME': None}}}}
	// Is multi path? False

	// Description accessor from parent Shape
	public void setPointDescription(String description, int ROIIndex, int shapeIndex)
	{
		description = filter? DataTools.sanitize(description) : description;
		store.setPointDescription(description, ROIIndex, shapeIndex);
	}

	// Ignoring Ellipse of parent abstract type
	// Fill accessor from parent Shape
	public void setPointFill(Integer fill, int ROIIndex, int shapeIndex)
	{
		store.setPointFill(fill, ROIIndex, shapeIndex);
	}

	// Ignoring FillRule of parent abstract type
	// Ignoring FontFamily of parent abstract type
	// FontSize accessor from parent Shape
	public void setPointFontSize(NonNegativeInteger fontSize, int ROIIndex, int shapeIndex)
	{
		store.setPointFontSize(fontSize, ROIIndex, shapeIndex);
	}

	// Ignoring FontStyle of parent abstract type
	// ID accessor from parent Shape
	public void setPointID(String id, int ROIIndex, int shapeIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setPointID(id, ROIIndex, shapeIndex);
	}

	// Label accessor from parent Shape
	public void setPointLabel(String label, int ROIIndex, int shapeIndex)
	{
		label = filter? DataTools.sanitize(label) : label;
		store.setPointLabel(label, ROIIndex, shapeIndex);
	}

	// Ignoring Line of parent abstract type
	// Ignoring LineCap of parent abstract type
	// Ignoring MarkerEnd of parent abstract type
	// Ignoring MarkerStart of parent abstract type
	// Ignoring Mask of parent abstract type
	// Name accessor from parent Shape
	public void setPointName(String name, int ROIIndex, int shapeIndex)
	{
		name = filter? DataTools.sanitize(name) : name;
		store.setPointName(name, ROIIndex, shapeIndex);
	}

	// Ignoring Path of parent abstract type
	// Ignoring Point of parent abstract type
	// Ignoring Polyline of parent abstract type
	// Ignoring Rectangle of parent abstract type
	// Stroke accessor from parent Shape
	public void setPointStroke(Integer stroke, int ROIIndex, int shapeIndex)
	{
		store.setPointStroke(stroke, ROIIndex, shapeIndex);
	}

	// StrokeDashArray accessor from parent Shape
	public void setPointStrokeDashArray(String strokeDashArray, int ROIIndex, int shapeIndex)
	{
		strokeDashArray = filter? DataTools.sanitize(strokeDashArray) : strokeDashArray;
		store.setPointStrokeDashArray(strokeDashArray, ROIIndex, shapeIndex);
	}

	// StrokeWidth accessor from parent Shape
	public void setPointStrokeWidth(Double strokeWidth, int ROIIndex, int shapeIndex)
	{
		store.setPointStrokeWidth(strokeWidth, ROIIndex, shapeIndex);
	}

	// Ignoring Text of parent abstract type
	// TheC accessor from parent Shape
	public void setPointTheC(NonNegativeInteger theC, int ROIIndex, int shapeIndex)
	{
		store.setPointTheC(theC, ROIIndex, shapeIndex);
	}

	// TheT accessor from parent Shape
	public void setPointTheT(NonNegativeInteger theT, int ROIIndex, int shapeIndex)
	{
		store.setPointTheT(theT, ROIIndex, shapeIndex);
	}

	// TheZ accessor from parent Shape
	public void setPointTheZ(NonNegativeInteger theZ, int ROIIndex, int shapeIndex)
	{
		store.setPointTheZ(theZ, ROIIndex, shapeIndex);
	}

	// Transform accessor from parent Shape
	public void setPointTransform(String transform, int ROIIndex, int shapeIndex)
	{
		transform = filter? DataTools.sanitize(transform) : transform;
		store.setPointTransform(transform, ROIIndex, shapeIndex);
	}

	public void setPointX(Double x, int ROIIndex, int shapeIndex)
	{
		store.setPointX(x, ROIIndex, shapeIndex);
	}

	public void setPointY(Double y, int ROIIndex, int shapeIndex)
	{
		store.setPointY(y, ROIIndex, shapeIndex);
	}

	//
	// Polyline property storage
	//
	// {u'Shape': {u'Union': {u'ROI': {u'OME': None}}}}
	// Is multi path? False

	// Description accessor from parent Shape
	public void setPolylineDescription(String description, int ROIIndex, int shapeIndex)
	{
		description = filter? DataTools.sanitize(description) : description;
		store.setPolylineDescription(description, ROIIndex, shapeIndex);
	}

	// Ignoring Ellipse of parent abstract type
	// Fill accessor from parent Shape
	public void setPolylineFill(Integer fill, int ROIIndex, int shapeIndex)
	{
		store.setPolylineFill(fill, ROIIndex, shapeIndex);
	}

	// Ignoring FillRule of parent abstract type
	// Ignoring FontFamily of parent abstract type
	// FontSize accessor from parent Shape
	public void setPolylineFontSize(NonNegativeInteger fontSize, int ROIIndex, int shapeIndex)
	{
		store.setPolylineFontSize(fontSize, ROIIndex, shapeIndex);
	}

	// Ignoring FontStyle of parent abstract type
	// ID accessor from parent Shape
	public void setPolylineID(String id, int ROIIndex, int shapeIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setPolylineID(id, ROIIndex, shapeIndex);
	}

	// Label accessor from parent Shape
	public void setPolylineLabel(String label, int ROIIndex, int shapeIndex)
	{
		label = filter? DataTools.sanitize(label) : label;
		store.setPolylineLabel(label, ROIIndex, shapeIndex);
	}

	// Ignoring Line of parent abstract type
	// Ignoring LineCap of parent abstract type
	// Ignoring MarkerEnd of parent abstract type
	// Ignoring MarkerStart of parent abstract type
	// Ignoring Mask of parent abstract type
	// Name accessor from parent Shape
	public void setPolylineName(String name, int ROIIndex, int shapeIndex)
	{
		name = filter? DataTools.sanitize(name) : name;
		store.setPolylineName(name, ROIIndex, shapeIndex);
	}

	// Ignoring Path of parent abstract type
	// Ignoring Point of parent abstract type
	// Ignoring Polyline of parent abstract type
	// Ignoring Rectangle of parent abstract type
	// Stroke accessor from parent Shape
	public void setPolylineStroke(Integer stroke, int ROIIndex, int shapeIndex)
	{
		store.setPolylineStroke(stroke, ROIIndex, shapeIndex);
	}

	// StrokeDashArray accessor from parent Shape
	public void setPolylineStrokeDashArray(String strokeDashArray, int ROIIndex, int shapeIndex)
	{
		strokeDashArray = filter? DataTools.sanitize(strokeDashArray) : strokeDashArray;
		store.setPolylineStrokeDashArray(strokeDashArray, ROIIndex, shapeIndex);
	}

	// StrokeWidth accessor from parent Shape
	public void setPolylineStrokeWidth(Double strokeWidth, int ROIIndex, int shapeIndex)
	{
		store.setPolylineStrokeWidth(strokeWidth, ROIIndex, shapeIndex);
	}

	// Ignoring Text of parent abstract type
	// TheC accessor from parent Shape
	public void setPolylineTheC(NonNegativeInteger theC, int ROIIndex, int shapeIndex)
	{
		store.setPolylineTheC(theC, ROIIndex, shapeIndex);
	}

	// TheT accessor from parent Shape
	public void setPolylineTheT(NonNegativeInteger theT, int ROIIndex, int shapeIndex)
	{
		store.setPolylineTheT(theT, ROIIndex, shapeIndex);
	}

	// TheZ accessor from parent Shape
	public void setPolylineTheZ(NonNegativeInteger theZ, int ROIIndex, int shapeIndex)
	{
		store.setPolylineTheZ(theZ, ROIIndex, shapeIndex);
	}

	// Transform accessor from parent Shape
	public void setPolylineTransform(String transform, int ROIIndex, int shapeIndex)
	{
		transform = filter? DataTools.sanitize(transform) : transform;
		store.setPolylineTransform(transform, ROIIndex, shapeIndex);
	}

	public void setPolylineClosed(Boolean closed, int ROIIndex, int shapeIndex)
	{
		store.setPolylineClosed(closed, ROIIndex, shapeIndex);
	}

	public void setPolylinePoints(String points, int ROIIndex, int shapeIndex)
	{
		points = filter? DataTools.sanitize(points) : points;
		store.setPolylinePoints(points, ROIIndex, shapeIndex);
	}

	//
	// Project property storage
	//
	// {u'OME': None}
	// Is multi path? False

	public void setProjectAnnotationRef(String annotation, int projectIndex, int annotationRefIndex)
	{
		store.setProjectAnnotationRef(annotation, projectIndex, annotationRefIndex);
	}

	// Ignoring Dataset_BackReference back reference
	public void setProjectDescription(String description, int projectIndex)
	{
		description = filter? DataTools.sanitize(description) : description;
		store.setProjectDescription(description, projectIndex);
	}

	public void setProjectExperimenterRef(String experimenter, int projectIndex)
	{
		store.setProjectExperimenterRef(experimenter, projectIndex);
	}

	public void setProjectGroupRef(String group, int projectIndex)
	{
		store.setProjectGroupRef(group, projectIndex);
	}

	public void setProjectID(String id, int projectIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setProjectID(id, projectIndex);
	}

	public void setProjectName(String name, int projectIndex)
	{
		name = filter? DataTools.sanitize(name) : name;
		store.setProjectName(name, projectIndex);
	}

	//
	// ProjectRef property storage
	//
	// {u'Dataset': {u'OME': None}}
	// Is multi path? False

	// 1:1
	// Is multi path? False
	// Ignoring ID property of reference ProjectRef

	//
	// Pump property storage
	//
	// {u'Laser': {u'LightSource': {u'Instrument': {u'OME': None}}}}
	// Is multi path? False

	// 1:1
	// Is multi path? False
	// Ignoring ID property of reference Pump

	//
	// ROI property storage
	//
	// {u'OME': None}
	// Is multi path? False

	public void setROIAnnotationRef(String annotation, int ROIIndex, int annotationRefIndex)
	{
		store.setROIAnnotationRef(annotation, ROIIndex, annotationRefIndex);
	}

	public void setROIDescription(String description, int ROIIndex)
	{
		description = filter? DataTools.sanitize(description) : description;
		store.setROIDescription(description, ROIIndex);
	}

	public void setROIID(String id, int ROIIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setROIID(id, ROIIndex);
	}

	// Ignoring Image_BackReference back reference
	// Ignoring MicrobeamManipulation_BackReference back reference
	public void setROIName(String name, int ROIIndex)
	{
		name = filter? DataTools.sanitize(name) : name;
		store.setROIName(name, ROIIndex);
	}

	public void setROINamespace(String namespace, int ROIIndex)
	{
		namespace = filter? DataTools.sanitize(namespace) : namespace;
		store.setROINamespace(namespace, ROIIndex);
	}

	// Ignoring Union element, complex property
	//
	// ROIRef property storage
	//
	// {u'Image': {u'OME': None}, u'MicrobeamManipulation': {u'Experiment': {u'OME': None}}}
	// Is multi path? True

	// 1:1
	// Is multi path? True
	// Ignoring ID property of reference ROIRef

	//
	// Reagent property storage
	//
	// {u'Screen': {u'OME': None}}
	// Is multi path? False

	public void setReagentAnnotationRef(String annotation, int screenIndex, int reagentIndex, int annotationRefIndex)
	{
		store.setReagentAnnotationRef(annotation, screenIndex, reagentIndex, annotationRefIndex);
	}

	public void setReagentDescription(String description, int screenIndex, int reagentIndex)
	{
		description = filter? DataTools.sanitize(description) : description;
		store.setReagentDescription(description, screenIndex, reagentIndex);
	}

	public void setReagentID(String id, int screenIndex, int reagentIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setReagentID(id, screenIndex, reagentIndex);
	}

	public void setReagentName(String name, int screenIndex, int reagentIndex)
	{
		name = filter? DataTools.sanitize(name) : name;
		store.setReagentName(name, screenIndex, reagentIndex);
	}

	public void setReagentReagentIdentifier(String reagentIdentifier, int screenIndex, int reagentIndex)
	{
		reagentIdentifier = filter? DataTools.sanitize(reagentIdentifier) : reagentIdentifier;
		store.setReagentReagentIdentifier(reagentIdentifier, screenIndex, reagentIndex);
	}

	// Ignoring Well_BackReference back reference
	//
	// ReagentRef property storage
	//
	// {u'Well': {u'Plate': {u'OME': None}}}
	// Is multi path? False

	// 1:1
	// Is multi path? False
	// Ignoring ID property of reference ReagentRef

	//
	// Rectangle property storage
	//
	// {u'Shape': {u'Union': {u'ROI': {u'OME': None}}}}
	// Is multi path? False

	// Description accessor from parent Shape
	public void setRectangleDescription(String description, int ROIIndex, int shapeIndex)
	{
		description = filter? DataTools.sanitize(description) : description;
		store.setRectangleDescription(description, ROIIndex, shapeIndex);
	}

	// Ignoring Ellipse of parent abstract type
	// Fill accessor from parent Shape
	public void setRectangleFill(Integer fill, int ROIIndex, int shapeIndex)
	{
		store.setRectangleFill(fill, ROIIndex, shapeIndex);
	}

	// Ignoring FillRule of parent abstract type
	// Ignoring FontFamily of parent abstract type
	// FontSize accessor from parent Shape
	public void setRectangleFontSize(NonNegativeInteger fontSize, int ROIIndex, int shapeIndex)
	{
		store.setRectangleFontSize(fontSize, ROIIndex, shapeIndex);
	}

	// Ignoring FontStyle of parent abstract type
	// ID accessor from parent Shape
	public void setRectangleID(String id, int ROIIndex, int shapeIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setRectangleID(id, ROIIndex, shapeIndex);
	}

	// Label accessor from parent Shape
	public void setRectangleLabel(String label, int ROIIndex, int shapeIndex)
	{
		label = filter? DataTools.sanitize(label) : label;
		store.setRectangleLabel(label, ROIIndex, shapeIndex);
	}

	// Ignoring Line of parent abstract type
	// Ignoring LineCap of parent abstract type
	// Ignoring MarkerEnd of parent abstract type
	// Ignoring MarkerStart of parent abstract type
	// Ignoring Mask of parent abstract type
	// Name accessor from parent Shape
	public void setRectangleName(String name, int ROIIndex, int shapeIndex)
	{
		name = filter? DataTools.sanitize(name) : name;
		store.setRectangleName(name, ROIIndex, shapeIndex);
	}

	// Ignoring Path of parent abstract type
	// Ignoring Point of parent abstract type
	// Ignoring Polyline of parent abstract type
	// Ignoring Rectangle of parent abstract type
	// Stroke accessor from parent Shape
	public void setRectangleStroke(Integer stroke, int ROIIndex, int shapeIndex)
	{
		store.setRectangleStroke(stroke, ROIIndex, shapeIndex);
	}

	// StrokeDashArray accessor from parent Shape
	public void setRectangleStrokeDashArray(String strokeDashArray, int ROIIndex, int shapeIndex)
	{
		strokeDashArray = filter? DataTools.sanitize(strokeDashArray) : strokeDashArray;
		store.setRectangleStrokeDashArray(strokeDashArray, ROIIndex, shapeIndex);
	}

	// StrokeWidth accessor from parent Shape
	public void setRectangleStrokeWidth(Double strokeWidth, int ROIIndex, int shapeIndex)
	{
		store.setRectangleStrokeWidth(strokeWidth, ROIIndex, shapeIndex);
	}

	// Ignoring Text of parent abstract type
	// TheC accessor from parent Shape
	public void setRectangleTheC(NonNegativeInteger theC, int ROIIndex, int shapeIndex)
	{
		store.setRectangleTheC(theC, ROIIndex, shapeIndex);
	}

	// TheT accessor from parent Shape
	public void setRectangleTheT(NonNegativeInteger theT, int ROIIndex, int shapeIndex)
	{
		store.setRectangleTheT(theT, ROIIndex, shapeIndex);
	}

	// TheZ accessor from parent Shape
	public void setRectangleTheZ(NonNegativeInteger theZ, int ROIIndex, int shapeIndex)
	{
		store.setRectangleTheZ(theZ, ROIIndex, shapeIndex);
	}

	// Transform accessor from parent Shape
	public void setRectangleTransform(String transform, int ROIIndex, int shapeIndex)
	{
		transform = filter? DataTools.sanitize(transform) : transform;
		store.setRectangleTransform(transform, ROIIndex, shapeIndex);
	}

	public void setRectangleHeight(Double height, int ROIIndex, int shapeIndex)
	{
		store.setRectangleHeight(height, ROIIndex, shapeIndex);
	}

	public void setRectangleWidth(Double width, int ROIIndex, int shapeIndex)
	{
		store.setRectangleWidth(width, ROIIndex, shapeIndex);
	}

	public void setRectangleX(Double x, int ROIIndex, int shapeIndex)
	{
		store.setRectangleX(x, ROIIndex, shapeIndex);
	}

	public void setRectangleY(Double y, int ROIIndex, int shapeIndex)
	{
		store.setRectangleY(y, ROIIndex, shapeIndex);
	}

	//
	// Screen property storage
	//
	// {u'OME': None}
	// Is multi path? False

	public void setScreenAnnotationRef(String annotation, int screenIndex, int annotationRefIndex)
	{
		store.setScreenAnnotationRef(annotation, screenIndex, annotationRefIndex);
	}

	public void setScreenDescription(String description, int screenIndex)
	{
		description = filter? DataTools.sanitize(description) : description;
		store.setScreenDescription(description, screenIndex);
	}

	public void setScreenID(String id, int screenIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setScreenID(id, screenIndex);
	}

	public void setScreenName(String name, int screenIndex)
	{
		name = filter? DataTools.sanitize(name) : name;
		store.setScreenName(name, screenIndex);
	}

	public void setScreenPlateRef(String plate, int screenIndex, int plateRefIndex)
	{
		store.setScreenPlateRef(plate, screenIndex, plateRefIndex);
	}

	public void setScreenProtocolDescription(String protocolDescription, int screenIndex)
	{
		protocolDescription = filter? DataTools.sanitize(protocolDescription) : protocolDescription;
		store.setScreenProtocolDescription(protocolDescription, screenIndex);
	}

	public void setScreenProtocolIdentifier(String protocolIdentifier, int screenIndex)
	{
		protocolIdentifier = filter? DataTools.sanitize(protocolIdentifier) : protocolIdentifier;
		store.setScreenProtocolIdentifier(protocolIdentifier, screenIndex);
	}

	// Ignoring Reagent element, complex property
	public void setScreenReagentSetDescription(String reagentSetDescription, int screenIndex)
	{
		reagentSetDescription = filter? DataTools.sanitize(reagentSetDescription) : reagentSetDescription;
		store.setScreenReagentSetDescription(reagentSetDescription, screenIndex);
	}

	public void setScreenReagentSetIdentifier(String reagentSetIdentifier, int screenIndex)
	{
		reagentSetIdentifier = filter? DataTools.sanitize(reagentSetIdentifier) : reagentSetIdentifier;
		store.setScreenReagentSetIdentifier(reagentSetIdentifier, screenIndex);
	}

	public void setScreenType(String type, int screenIndex)
	{
		type = filter? DataTools.sanitize(type) : type;
		store.setScreenType(type, screenIndex);
	}

	//
	// ScreenRef property storage
	//
	// {u'Plate': {u'OME': None}}
	// Is multi path? False

	// 1:1
	// Is multi path? False
	// Ignoring ID property of reference ScreenRef

	//
	// StageLabel property storage
	//
	// {u'Image': {u'OME': None}}
	// Is multi path? False

	public void setStageLabelName(String name, int imageIndex)
	{
		name = filter? DataTools.sanitize(name) : name;
		store.setStageLabelName(name, imageIndex);
	}

	public void setStageLabelX(Double x, int imageIndex)
	{
		store.setStageLabelX(x, imageIndex);
	}

	public void setStageLabelY(Double y, int imageIndex)
	{
		store.setStageLabelY(y, imageIndex);
	}

	public void setStageLabelZ(Double z, int imageIndex)
	{
		store.setStageLabelZ(z, imageIndex);
	}

	//
	// StructuredAnnotations property storage
	//
	// {u'OME': None}
	// Is multi path? False

	// Ignoring BooleanAnnotation element, complex property
	// Ignoring CommentAnnotation element, complex property
	// Ignoring DoubleAnnotation element, complex property
	// Ignoring FileAnnotation element, complex property
	// Ignoring ListAnnotation element, complex property
	// Ignoring LongAnnotation element, complex property
	// Ignoring TagAnnotation element, complex property
	// Ignoring TermAnnotation element, complex property
	// Ignoring TimestampAnnotation element, complex property
	// Ignoring XMLAnnotation element, complex property
	//
	// TagAnnotation property storage
	//
	// {u'StructuredAnnotations': {u'OME': None}}
	// Is multi path? False

	public void setTagAnnotationAnnotationRef(String annotation, int tagAnnotationIndex, int annotationRefIndex)
	{
		store.setTagAnnotationAnnotationRef(annotation, tagAnnotationIndex, annotationRefIndex);
	}

	// Ignoring Channel_BackReference back reference
	// Ignoring Dataset_BackReference back reference
	public void setTagAnnotationDescription(String description, int tagAnnotationIndex)
	{
		description = filter? DataTools.sanitize(description) : description;
		store.setTagAnnotationDescription(description, tagAnnotationIndex);
	}

	// Ignoring Experimenter_BackReference back reference
	public void setTagAnnotationID(String id, int tagAnnotationIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setTagAnnotationID(id, tagAnnotationIndex);
	}

	// Ignoring Image_BackReference back reference
	public void setTagAnnotationNamespace(String namespace, int tagAnnotationIndex)
	{
		namespace = filter? DataTools.sanitize(namespace) : namespace;
		store.setTagAnnotationNamespace(namespace, tagAnnotationIndex);
	}

	// Ignoring Pixels_BackReference back reference
	// Ignoring Plane_BackReference back reference
	// Ignoring PlateAcquisition_BackReference back reference
	// Ignoring Plate_BackReference back reference
	// Ignoring Project_BackReference back reference
	// Ignoring ROI_BackReference back reference
	// Ignoring Reagent_BackReference back reference
	// Ignoring Screen_BackReference back reference
	public void setTagAnnotationValue(String value, int tagAnnotationIndex)
	{
		value = filter? DataTools.sanitize(value) : value;
		store.setTagAnnotationValue(value, tagAnnotationIndex);
	}

	// Ignoring WellSample_BackReference back reference
	// Ignoring Well_BackReference back reference
	//
	// TermAnnotation property storage
	//
	// {u'StructuredAnnotations': {u'OME': None}}
	// Is multi path? False

	public void setTermAnnotationAnnotationRef(String annotation, int termAnnotationIndex, int annotationRefIndex)
	{
		store.setTermAnnotationAnnotationRef(annotation, termAnnotationIndex, annotationRefIndex);
	}

	// Ignoring Channel_BackReference back reference
	// Ignoring Dataset_BackReference back reference
	public void setTermAnnotationDescription(String description, int termAnnotationIndex)
	{
		description = filter? DataTools.sanitize(description) : description;
		store.setTermAnnotationDescription(description, termAnnotationIndex);
	}

	// Ignoring Experimenter_BackReference back reference
	public void setTermAnnotationID(String id, int termAnnotationIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setTermAnnotationID(id, termAnnotationIndex);
	}

	// Ignoring Image_BackReference back reference
	public void setTermAnnotationNamespace(String namespace, int termAnnotationIndex)
	{
		namespace = filter? DataTools.sanitize(namespace) : namespace;
		store.setTermAnnotationNamespace(namespace, termAnnotationIndex);
	}

	// Ignoring Pixels_BackReference back reference
	// Ignoring Plane_BackReference back reference
	// Ignoring PlateAcquisition_BackReference back reference
	// Ignoring Plate_BackReference back reference
	// Ignoring Project_BackReference back reference
	// Ignoring ROI_BackReference back reference
	// Ignoring Reagent_BackReference back reference
	// Ignoring Screen_BackReference back reference
	public void setTermAnnotationValue(String value, int termAnnotationIndex)
	{
		value = filter? DataTools.sanitize(value) : value;
		store.setTermAnnotationValue(value, termAnnotationIndex);
	}

	// Ignoring WellSample_BackReference back reference
	// Ignoring Well_BackReference back reference
	//
	// Text property storage
	//
	// {u'Shape': {u'Union': {u'ROI': {u'OME': None}}}}
	// Is multi path? False

	// Description accessor from parent Shape
	public void setTextDescription(String description, int ROIIndex, int shapeIndex)
	{
		description = filter? DataTools.sanitize(description) : description;
		store.setTextDescription(description, ROIIndex, shapeIndex);
	}

	// Ignoring Ellipse of parent abstract type
	// Fill accessor from parent Shape
	public void setTextFill(Integer fill, int ROIIndex, int shapeIndex)
	{
		store.setTextFill(fill, ROIIndex, shapeIndex);
	}

	// Ignoring FillRule of parent abstract type
	// Ignoring FontFamily of parent abstract type
	// FontSize accessor from parent Shape
	public void setTextFontSize(NonNegativeInteger fontSize, int ROIIndex, int shapeIndex)
	{
		store.setTextFontSize(fontSize, ROIIndex, shapeIndex);
	}

	// Ignoring FontStyle of parent abstract type
	// ID accessor from parent Shape
	public void setTextID(String id, int ROIIndex, int shapeIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setTextID(id, ROIIndex, shapeIndex);
	}

	// Label accessor from parent Shape
	public void setTextLabel(String label, int ROIIndex, int shapeIndex)
	{
		label = filter? DataTools.sanitize(label) : label;
		store.setTextLabel(label, ROIIndex, shapeIndex);
	}

	// Ignoring Line of parent abstract type
	// Ignoring LineCap of parent abstract type
	// Ignoring MarkerEnd of parent abstract type
	// Ignoring MarkerStart of parent abstract type
	// Ignoring Mask of parent abstract type
	// Name accessor from parent Shape
	public void setTextName(String name, int ROIIndex, int shapeIndex)
	{
		name = filter? DataTools.sanitize(name) : name;
		store.setTextName(name, ROIIndex, shapeIndex);
	}

	// Ignoring Path of parent abstract type
	// Ignoring Point of parent abstract type
	// Ignoring Polyline of parent abstract type
	// Ignoring Rectangle of parent abstract type
	// Stroke accessor from parent Shape
	public void setTextStroke(Integer stroke, int ROIIndex, int shapeIndex)
	{
		store.setTextStroke(stroke, ROIIndex, shapeIndex);
	}

	// StrokeDashArray accessor from parent Shape
	public void setTextStrokeDashArray(String strokeDashArray, int ROIIndex, int shapeIndex)
	{
		strokeDashArray = filter? DataTools.sanitize(strokeDashArray) : strokeDashArray;
		store.setTextStrokeDashArray(strokeDashArray, ROIIndex, shapeIndex);
	}

	// StrokeWidth accessor from parent Shape
	public void setTextStrokeWidth(Double strokeWidth, int ROIIndex, int shapeIndex)
	{
		store.setTextStrokeWidth(strokeWidth, ROIIndex, shapeIndex);
	}

	// Ignoring Text of parent abstract type
	// TheC accessor from parent Shape
	public void setTextTheC(NonNegativeInteger theC, int ROIIndex, int shapeIndex)
	{
		store.setTextTheC(theC, ROIIndex, shapeIndex);
	}

	// TheT accessor from parent Shape
	public void setTextTheT(NonNegativeInteger theT, int ROIIndex, int shapeIndex)
	{
		store.setTextTheT(theT, ROIIndex, shapeIndex);
	}

	// TheZ accessor from parent Shape
	public void setTextTheZ(NonNegativeInteger theZ, int ROIIndex, int shapeIndex)
	{
		store.setTextTheZ(theZ, ROIIndex, shapeIndex);
	}

	// Transform accessor from parent Shape
	public void setTextTransform(String transform, int ROIIndex, int shapeIndex)
	{
		transform = filter? DataTools.sanitize(transform) : transform;
		store.setTextTransform(transform, ROIIndex, shapeIndex);
	}

	public void setTextValue(String value, int ROIIndex, int shapeIndex)
	{
		value = filter? DataTools.sanitize(value) : value;
		store.setTextValue(value, ROIIndex, shapeIndex);
	}

	public void setTextX(Double x, int ROIIndex, int shapeIndex)
	{
		store.setTextX(x, ROIIndex, shapeIndex);
	}

	public void setTextY(Double y, int ROIIndex, int shapeIndex)
	{
		store.setTextY(y, ROIIndex, shapeIndex);
	}

	//
	// TiffData property storage
	//
	// {u'Pixels': {u'Image': {u'OME': None}}}
	// Is multi path? False

	public void setTiffDataFirstC(NonNegativeInteger firstC, int imageIndex, int tiffDataIndex)
	{
		store.setTiffDataFirstC(firstC, imageIndex, tiffDataIndex);
	}

	public void setTiffDataFirstT(NonNegativeInteger firstT, int imageIndex, int tiffDataIndex)
	{
		store.setTiffDataFirstT(firstT, imageIndex, tiffDataIndex);
	}

	public void setTiffDataFirstZ(NonNegativeInteger firstZ, int imageIndex, int tiffDataIndex)
	{
		store.setTiffDataFirstZ(firstZ, imageIndex, tiffDataIndex);
	}

	public void setTiffDataIFD(NonNegativeInteger ifd, int imageIndex, int tiffDataIndex)
	{
		store.setTiffDataIFD(ifd, imageIndex, tiffDataIndex);
	}

	public void setTiffDataPlaneCount(NonNegativeInteger planeCount, int imageIndex, int tiffDataIndex)
	{
		store.setTiffDataPlaneCount(planeCount, imageIndex, tiffDataIndex);
	}

	// Ignoring UUID element, complex property
	//
	// TimestampAnnotation property storage
	//
	// {u'StructuredAnnotations': {u'OME': None}}
	// Is multi path? False

	public void setTimestampAnnotationAnnotationRef(String annotation, int timestampAnnotationIndex, int annotationRefIndex)
	{
		store.setTimestampAnnotationAnnotationRef(annotation, timestampAnnotationIndex, annotationRefIndex);
	}

	// Ignoring Channel_BackReference back reference
	// Ignoring Dataset_BackReference back reference
	public void setTimestampAnnotationDescription(String description, int timestampAnnotationIndex)
	{
		description = filter? DataTools.sanitize(description) : description;
		store.setTimestampAnnotationDescription(description, timestampAnnotationIndex);
	}

	// Ignoring Experimenter_BackReference back reference
	public void setTimestampAnnotationID(String id, int timestampAnnotationIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setTimestampAnnotationID(id, timestampAnnotationIndex);
	}

	// Ignoring Image_BackReference back reference
	public void setTimestampAnnotationNamespace(String namespace, int timestampAnnotationIndex)
	{
		namespace = filter? DataTools.sanitize(namespace) : namespace;
		store.setTimestampAnnotationNamespace(namespace, timestampAnnotationIndex);
	}

	// Ignoring Pixels_BackReference back reference
	// Ignoring Plane_BackReference back reference
	// Ignoring PlateAcquisition_BackReference back reference
	// Ignoring Plate_BackReference back reference
	// Ignoring Project_BackReference back reference
	// Ignoring ROI_BackReference back reference
	// Ignoring Reagent_BackReference back reference
	// Ignoring Screen_BackReference back reference
	public void setTimestampAnnotationValue(String value, int timestampAnnotationIndex)
	{
		value = filter? DataTools.sanitize(value) : value;
		store.setTimestampAnnotationValue(value, timestampAnnotationIndex);
	}

	// Ignoring WellSample_BackReference back reference
	// Ignoring Well_BackReference back reference
	//
	// TransmittanceRange property storage
	//
	// {u'Filter': {u'Instrument': {u'OME': None}}}
	// Is multi path? False

	public void setTransmittanceRangeCutIn(PositiveInteger cutIn, int instrumentIndex, int filterIndex)
	{
		store.setTransmittanceRangeCutIn(cutIn, instrumentIndex, filterIndex);
	}

	public void setTransmittanceRangeCutInTolerance(NonNegativeInteger cutInTolerance, int instrumentIndex, int filterIndex)
	{
		store.setTransmittanceRangeCutInTolerance(cutInTolerance, instrumentIndex, filterIndex);
	}

	public void setTransmittanceRangeCutOut(PositiveInteger cutOut, int instrumentIndex, int filterIndex)
	{
		store.setTransmittanceRangeCutOut(cutOut, instrumentIndex, filterIndex);
	}

	public void setTransmittanceRangeCutOutTolerance(NonNegativeInteger cutOutTolerance, int instrumentIndex, int filterIndex)
	{
		store.setTransmittanceRangeCutOutTolerance(cutOutTolerance, instrumentIndex, filterIndex);
	}

	public void setTransmittanceRangeTransmittance(PercentFraction transmittance, int instrumentIndex, int filterIndex)
	{
		store.setTransmittanceRangeTransmittance(transmittance, instrumentIndex, filterIndex);
	}

	// Element's text data
	// {u'TiffData': [u'int imageIndex', u'int tiffDataIndex']}
	public void setUUIDValue(String value, int imageIndex, int tiffDataIndex)
	{
		value = filter? DataTools.sanitize(value) : value;
		store.setUUIDValue(value, imageIndex, tiffDataIndex);
	}

	//
	// UUID property storage
	//
	// {u'TiffData': {u'Pixels': {u'Image': {u'OME': None}}}}
	// Is multi path? False

	public void setUUIDFileName(String fileName, int imageIndex, int tiffDataIndex)
	{
		fileName = filter? DataTools.sanitize(fileName) : fileName;
		store.setUUIDFileName(fileName, imageIndex, tiffDataIndex);
	}

	//
	// Union property storage
	//
	// {u'ROI': {u'OME': None}}
	// Is multi path? False

	// Ignoring Shape element, complex property
	//
	// Well property storage
	//
	// {u'Plate': {u'OME': None}}
	// Is multi path? False

	public void setWellAnnotationRef(String annotation, int plateIndex, int wellIndex, int annotationRefIndex)
	{
		store.setWellAnnotationRef(annotation, plateIndex, wellIndex, annotationRefIndex);
	}

	public void setWellColor(Integer color, int plateIndex, int wellIndex)
	{
		store.setWellColor(color, plateIndex, wellIndex);
	}

	public void setWellColumn(NonNegativeInteger column, int plateIndex, int wellIndex)
	{
		store.setWellColumn(column, plateIndex, wellIndex);
	}

	public void setWellExternalDescription(String externalDescription, int plateIndex, int wellIndex)
	{
		externalDescription = filter? DataTools.sanitize(externalDescription) : externalDescription;
		store.setWellExternalDescription(externalDescription, plateIndex, wellIndex);
	}

	public void setWellExternalIdentifier(String externalIdentifier, int plateIndex, int wellIndex)
	{
		externalIdentifier = filter? DataTools.sanitize(externalIdentifier) : externalIdentifier;
		store.setWellExternalIdentifier(externalIdentifier, plateIndex, wellIndex);
	}

	public void setWellID(String id, int plateIndex, int wellIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setWellID(id, plateIndex, wellIndex);
	}

	public void setWellReagentRef(String reagent, int plateIndex, int wellIndex)
	{
		store.setWellReagentRef(reagent, plateIndex, wellIndex);
	}

	public void setWellRow(NonNegativeInteger row, int plateIndex, int wellIndex)
	{
		store.setWellRow(row, plateIndex, wellIndex);
	}

	public void setWellStatus(String status, int plateIndex, int wellIndex)
	{
		status = filter? DataTools.sanitize(status) : status;
		store.setWellStatus(status, plateIndex, wellIndex);
	}

	// Ignoring WellSample element, complex property
	//
	// WellSample property storage
	//
	// {u'Well': {u'Plate': {u'OME': None}}}
	// Is multi path? False

	public void setWellSampleAnnotationRef(String annotation, int plateIndex, int wellIndex, int wellSampleIndex, int annotationRefIndex)
	{
		store.setWellSampleAnnotationRef(annotation, plateIndex, wellIndex, wellSampleIndex, annotationRefIndex);
	}

	public void setWellSampleID(String id, int plateIndex, int wellIndex, int wellSampleIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setWellSampleID(id, plateIndex, wellIndex, wellSampleIndex);
	}

	public void setWellSampleImageRef(String image, int plateIndex, int wellIndex, int wellSampleIndex)
	{
		store.setWellSampleImageRef(image, plateIndex, wellIndex, wellSampleIndex);
	}

	public void setWellSampleIndex(NonNegativeInteger index, int plateIndex, int wellIndex, int wellSampleIndex)
	{
		store.setWellSampleIndex(index, plateIndex, wellIndex, wellSampleIndex);
	}

	// Ignoring PlateAcquisition_BackReference back reference
	public void setWellSamplePositionX(Double positionX, int plateIndex, int wellIndex, int wellSampleIndex)
	{
		store.setWellSamplePositionX(positionX, plateIndex, wellIndex, wellSampleIndex);
	}

	public void setWellSamplePositionY(Double positionY, int plateIndex, int wellIndex, int wellSampleIndex)
	{
		store.setWellSamplePositionY(positionY, plateIndex, wellIndex, wellSampleIndex);
	}

	public void setWellSampleTimepoint(String timepoint, int plateIndex, int wellIndex, int wellSampleIndex)
	{
		timepoint = filter? DataTools.sanitize(timepoint) : timepoint;
		store.setWellSampleTimepoint(timepoint, plateIndex, wellIndex, wellSampleIndex);
	}

	//
	// WellSampleRef property storage
	//
	// {u'PlateAcquisition': {u'Plate': {u'OME': None}}}
	// Is multi path? False

	// 1:1
	// Is multi path? False
	// Ignoring ID property of reference WellSampleRef

	//
	// XMLAnnotation property storage
	//
	// {u'StructuredAnnotations': {u'OME': None}}
	// Is multi path? False

	public void setXMLAnnotationAnnotationRef(String annotation, int XMLAnnotationIndex, int annotationRefIndex)
	{
		store.setXMLAnnotationAnnotationRef(annotation, XMLAnnotationIndex, annotationRefIndex);
	}

	// Ignoring Channel_BackReference back reference
	// Ignoring Dataset_BackReference back reference
	public void setXMLAnnotationDescription(String description, int XMLAnnotationIndex)
	{
		description = filter? DataTools.sanitize(description) : description;
		store.setXMLAnnotationDescription(description, XMLAnnotationIndex);
	}

	// Ignoring Experimenter_BackReference back reference
	public void setXMLAnnotationID(String id, int XMLAnnotationIndex)
	{
		id = filter? DataTools.sanitize(id) : id;
		store.setXMLAnnotationID(id, XMLAnnotationIndex);
	}

	// Ignoring Image_BackReference back reference
	public void setXMLAnnotationNamespace(String namespace, int XMLAnnotationIndex)
	{
		namespace = filter? DataTools.sanitize(namespace) : namespace;
		store.setXMLAnnotationNamespace(namespace, XMLAnnotationIndex);
	}

	// Ignoring Pixels_BackReference back reference
	// Ignoring Plane_BackReference back reference
	// Ignoring PlateAcquisition_BackReference back reference
	// Ignoring Plate_BackReference back reference
	// Ignoring Project_BackReference back reference
	// Ignoring ROI_BackReference back reference
	// Ignoring Reagent_BackReference back reference
	// Ignoring Screen_BackReference back reference
	public void setXMLAnnotationValue(String value, int XMLAnnotationIndex)
	{
		value = filter? DataTools.sanitize(value) : value;
		store.setXMLAnnotationValue(value, XMLAnnotationIndex);
	}

	// Ignoring WellSample_BackReference back reference
	// Ignoring Well_BackReference back reference
}

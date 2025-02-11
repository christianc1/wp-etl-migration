<?php
/**
 * Interface: Transformation Pipeline
 *
 * Interface for transformation pipelines that process data during ETL operations.
 *
 * @package TenupETL\Classes\Transform\TransformationPipelines
 */

namespace TenupETL\Classes\Transform\TransformationPipelines;

use Flow\ETL\DataFrame;
use TenupETL\Classes\Load\LedgerRegistry;
interface TransformationPipeline {
	/**
	 * Constructor.
	 *
	 * @param DataFrame      $state           The initial state of the transformation pipeline.
	 * @param LedgerRegistry $ledger_registry The ledger registry.
	 */
	public function __construct( DataFrame $state, LedgerRegistry $ledger_registry );

	/**
	 * Run the transformation pipeline.
	 *
	 * @return TransformationPipeline The current pipeline instance.
	 */
	public function run(): TransformationPipeline;

	/**
	 * Prepare the transformation pipeline before running.
	 *
	 * @return TransformationPipeline The current pipeline instance.
	 */
	public function prepare(): TransformationPipeline;

	/**
	 * Finalize the transformation pipeline after running.
	 *
	 * @return TransformationPipeline The current pipeline instance.
	 */
	public function finalize(): TransformationPipeline;

	/**
	 * Get the current state of the transformation pipeline.
	 *
	 * @return DataFrame The current state.
	 */
	public function get_current_state(): DataFrame;

	/**
	 * Get the final state after running the transformation pipeline.
	 *
	 * @return DataFrame The final state.
	 */
	public function get_final_state(): DataFrame;
}
